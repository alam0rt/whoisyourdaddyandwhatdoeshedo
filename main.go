package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"golang.org/x/exp/maps"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

var ignoreGroups = []string{
	"iam.aws.zendesk.com",
}

const restoreFlag = `--restore-resource-priorities`

// https://velero.io/docs/v1.15/restore-reference/#restore-order
var defaultOrder = []string{
	"customresourcedefinitions",
	"namespaces,storageclasses",
	"volumesnapshotclass.snapshot.storage.k8s.io",
	"volumesnapshotcontents.snapshot.storage.k8s.io",
	"volumesnapshots.snapshot.storage.k8s.io",
	"persistentvolumes,persistentvolumeclaims",
	"secrets",
	"configmaps",
	"serviceaccounts",
	"limitranges",
	"pods",
	"replicasets.apps",
	"clusters.cluster.x-k8s.io",
	"clusterresourcesets.addons.cluster.x-k8s.io",
}

var (
	crdRes schema.GroupVersionResource = schema.GroupVersionResource{
		Group:    "apiextensions.k8s.io",
		Version:  "v1",
		Resource: "customresourcedefinitions",
	}
)

type GVK struct {
	GVR  schema.GroupVersionResource
	Kind string
}

// for a custom resource, get its GVK and whether it is namespaced
func getRes(in unstructured.Unstructured) (GVK, bool, error) {
	if in.DeepCopy() == nil {
		return GVK{}, false, fmt.Errorf("cannot get resource from nil object")
	}

	if in.GetKind() != "CustomResourceDefinition" {
		return GVK{}, false, fmt.Errorf("cannot get resource from non-CRD object %s", in.GetKind())
	}

	group := in.Object["spec"].(map[string]interface{})["group"].(string)
	kind := in.Object["spec"].(map[string]interface{})["names"].(map[string]interface{})["kind"].(string)
	plural := in.Object["spec"].(map[string]interface{})["names"].(map[string]interface{})["plural"].(string)
	versionsSpec := in.Object["spec"].(map[string]interface{})["versions"].([]interface{})
	versions := []string{}
	for _, version := range versionsSpec {
		version := version.(map[string]interface{})
		versions = append(versions, version["name"].(string))
	}
	namespaced := in.Object["spec"].(map[string]interface{})["scope"].(string) == "Namespaced"

	return GVK{
		GVR: schema.GroupVersionResource{
			Group:    group,
			Version:  versions[len(versions)-1], // last version is the most recent
			Resource: plural,
		},
		Kind: kind,
	}, namespaced, nil
}

func main() {
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}

	user := flag.String("as", "", "user to impersonate")
	group := flag.String("as-group", "", "group to impersonate")
	flag.Parse()

	ctx := context.Background()

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		slog.Error("cannot build client", "error", err)
		os.Exit(1)
	}

	config.Impersonate = rest.ImpersonationConfig{}

	if group != nil {
		config.Impersonate.Groups = []string{*group}
	}

	if user != nil {
		config.Impersonate.UserName = *user
	}

	// create the clientset
	clientset, err := dynamic.NewForConfig(config)
	if err != nil {
		slog.Error("cannot create client", "error", err)
		os.Exit(1)
	}

	crds, err := clientset.Resource(crdRes).List(ctx, v1.ListOptions{})
	if err != nil {
		slog.Error("cannot list CRDs", "error", err)
		os.Exit(1)
	}

	// all groups contained in CRDs
	allGroups := []string{}
	crdToKind := map[string]string{}
	for _, crd := range crds.Items {
		res, _, err := getRes(crd)
		if err != nil {
			slog.Error("cannot get resource", "error", err)
			os.Exit(1)
		}
		if slices.Contains(ignoreGroups, res.GVR.Group) {
			continue
		}

		// a mapping of CRD names to the kinds exposed
		// (required because owner references are in the form of kind and need to be mapped to CRD names)
		// e.g. foo.bar.com -> Foo
		crdToKind[crd.GetName()] = res.Kind

		// all groups contained in CRDs
		allGroups = append(allGroups, res.GVR.GroupResource().Group)
	}

	// get every custom resource
	all, err := findAll(ctx, crds, clientset)
	if err != nil {
		slog.Error("cannot find resources", "error", err)
		os.Exit(1)
	}

	// get all resources that have owners
	// as these are the ones that need to be restored in a specific order
	result := map[string]map[string]any{}
	for _, res := range all {
		for i := range res.GetOwnerReferences() {
			// need to get the group of the owner by splitting the APIVersion
			group := strings.Split(res.GetOwnerReferences()[i].APIVersion, "/")[0]
			// if group is contained in allGroups, then it is a CRD
			if slices.Contains(allGroups, group) {
				// for every owner reference, add the resource to the map
				// so we can track the dependencies
				result[res.GetKind()] = map[string]any{
					res.GetOwnerReferences()[i].Kind: nil,
				}
			}
		}
	}

	// take every result and order it so resources with no owners are at the top
	// and resources that are owned by other resources are at the bottom
	// e.g. IAMRoles are owned by Nodegroups which are in turn owned by NodegroupDeployments
	// so the order should be NodegroupDeployments -> Nodegroups -> IAMRoles
	final := []string{}
	ordered := orderDependencies(result)
	for _, depend := range ordered {
		for k, v := range crdToKind {
			if result[v] == nil {
				// remove any resources that are not in the CRD list
				// as these do not have owners and thus will get restored
				// after.
				continue
			}
			if v == depend {
				final = append(final, k)
			}
		}
	}

	// add final order to end of default order
	v := append(defaultOrder, final...)
	fmt.Printf("%s=%s\n", restoreFlag, strings.Join(v, ","))
}

// findAll finds all resources of given CRDs
func findAll(ctx context.Context, crds *unstructured.UnstructuredList, clientset dynamic.Interface) ([]unstructured.Unstructured, error) {
	allResources := []unstructured.Unstructured{}
	if crds == nil {
		return nil, fmt.Errorf("cannot find resources from nil object")
	}
	wg := sync.WaitGroup{}
	wg.Add(len(crds.Items))
	for _, crd := range crds.Items {
		go func(crd unstructured.Unstructured) {
			defer wg.Done()

			res, namespaced, err := getRes(crd)
			if err != nil {
				return
			}

			// get all resources whether they are namespaced or not
			var list func(context.Context, v1.ListOptions) (*unstructured.UnstructuredList, error)
			if namespaced {
				list = clientset.Resource(res.GVR).Namespace("").List
			} else {
				list = clientset.Resource(res.GVR).List
			}

			// get all resources of this type
			resources, err := list(ctx, v1.ListOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				slog.Error("cannot list resources", "error", err)
				return
			}
			if apierrors.IsNotFound(err) {
				return
			}

			slog.Info("found resources", "kind", res.Kind, "count", len(resources.Items))

			allResources = append(allResources, resources.Items...)
		}(crd)
	}
	wg.Wait()
	return allResources, nil
}

func orderDependencies(data map[string]map[string]any) []string {
	all := map[string]int{}

	// get all keys
	for key, value := range data {
		all[key] = 0
		for k := range value {
			all[k]++
		}
	}

	// flip the map
	flipped := map[int][]string{}
	for key, value := range all {
		if _, ok := flipped[value]; !ok {
			flipped[value] = []string{}
		}
		flipped[value] = append(flipped[value], key)
	}

	order := maps.Keys(flipped)
	slices.Sort(order)

	result := []string{}
	for _, idx := range order {
		result = append(result, flipped[idx]...)
	}

	return result
}
