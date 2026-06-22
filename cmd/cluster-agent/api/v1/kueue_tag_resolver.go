// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

//go:build kubeapiserver

package v1

import (
	"slices"

	"github.com/gobwas/glob"

	k8smetadata "github.com/DataDog/datadog-agent/comp/core/tagger/k8s_metadata"
	"github.com/DataDog/datadog-agent/comp/core/tagger/taglist"
	workloadmeta "github.com/DataDog/datadog-agent/comp/core/workloadmeta/def"
	pkgconfigmodel "github.com/DataDog/datadog-agent/pkg/config/model"
	configutils "github.com/DataDog/datadog-agent/pkg/config/utils"
	"github.com/DataDog/datadog-agent/pkg/util/kubernetes"
)

// kueueResourcesMetadataAsTagsResolver applies kubernetes_resources_{labels,annotations}_as_tags
// on the cluster agent for Kueue LocalQueues, ClusterQueues, ResourceFlavors, and Workloads, so only
// the resulting tag strings are streamed to node agents (no raw label/annotation maps).
//
// Queue intrinsic tags (kueue_local_queue, kueue_cluster_queue, kube_namespace) and
// Workload/ResourceFlavor intrinsic tags are not included here; the node tagger derives those.
type kueueResourcesMetadataAsTagsResolver struct {
	labelsAsTags      map[string]map[string]string
	annotationsAsTags map[string]map[string]string
	globLabels        map[string]map[string]glob.Glob
	globAnnotations   map[string]map[string]glob.Glob
}

func newKueueResourcesMetadataAsTagsResolver(cfg pkgconfigmodel.Reader) *kueueResourcesMetadataAsTagsResolver {
	r := &kueueResourcesMetadataAsTagsResolver{
		labelsAsTags:      map[string]map[string]string{},
		annotationsAsTags: map[string]map[string]string{},
		globLabels:        map[string]map[string]glob.Glob{},
		globAnnotations:   map[string]map[string]glob.Glob{},
	}

	if cfg == nil {
		return r
	}

	metadataAsTags := configutils.GetMetadataAsTags(cfg)

	for resource, labelsAsTags := range metadataAsTags.GetResourcesLabelsAsTags() {
		r.labelsAsTags[resource], r.globLabels[resource] = k8smetadata.InitMetadataAsTags(labelsAsTags)
	}
	for resource, annotationsAsTags := range metadataAsTags.GetResourcesAnnotationsAsTags() {
		r.annotationsAsTags[resource], r.globAnnotations[resource] = k8smetadata.InitMetadataAsTags(annotationsAsTags)
	}

	return r
}

// resolveQueueMetadataAsTags returns tags from the queue's labels and annotations
// per kubernetes_resources_{labels,annotations}_as_tags. Each entry is "name:value";
// a leading '+' on the name marks a high-cardinality tag (taglist.AddAuto on the node).
func (r *kueueResourcesMetadataAsTagsResolver) resolveQueueMetadataAsTags(queue *workloadmeta.KubernetesKueueQueue) []string {
	if r == nil {
		return nil
	}

	groupResource := kueueQueueGroupResource(queue.QueueType)
	if groupResource == "" {
		return nil
	}

	return r.resolveMetadataAsTags(groupResource, queue.Labels, queue.Annotations)
}

// resolveResourceFlavorMetadataAsTags is like resolveQueueMetadataAsTags for ResourceFlavor
// objects (group resource resourceflavors.kueue.x-k8s.io).
func (r *kueueResourcesMetadataAsTagsResolver) resolveResourceFlavorMetadataAsTags(flavor *workloadmeta.KubernetesKueueResourceFlavor) []string {
	if r == nil {
		return nil
	}

	groupResource := kubernetes.KueueResourceFlavorResourceName + "." + kubernetes.KueueGroupName

	return r.resolveMetadataAsTags(groupResource, flavor.Labels, flavor.Annotations)
}

// resolveWorkloadMetadataAsTags is like resolveQueueMetadataAsTags for Workload
// objects (group resource workloads.kueue.x-k8s.io).
func (r *kueueResourcesMetadataAsTagsResolver) resolveWorkloadMetadataAsTags(workload *workloadmeta.KubernetesKueueWorkload) []string {
	if r == nil {
		return nil
	}

	groupResource := kubernetes.KueueWorkloadResourceName + "." + kubernetes.KueueGroupName

	return r.resolveMetadataAsTags(groupResource, workload.Labels, workload.Annotations)
}

func (r *kueueResourcesMetadataAsTagsResolver) resolveMetadataAsTags(groupResource string, labels, annotations map[string]string) []string {
	tagList := taglist.NewTagList()
	for name, value := range labels {
		k8smetadata.AddMetadataAsTags(name, value, r.labelsAsTags[groupResource], r.globLabels[groupResource], tagList)
	}
	for name, value := range annotations {
		k8smetadata.AddMetadataAsTags(name, value, r.annotationsAsTags[groupResource], r.globAnnotations[groupResource], tagList)
	}

	low, _, high, _ := tagList.Compute()
	if len(low)+len(high) == 0 {
		return nil
	}

	return sortedResolvedTags(low, high)
}

func sortedResolvedTags(low, high []string) []string {
	resolved := make([]string, 0, len(low)+len(high))
	resolved = append(resolved, low...)
	for _, tag := range high {
		resolved = append(resolved, "+"+tag)
	}
	// taglist.Compute returns map-backed slices; sort to keep stream diff
	// comparisons stable and avoid spurious metadata updates.
	slices.Sort(resolved)
	return resolved
}

func kueueQueueGroupResource(queueType workloadmeta.KueueQueueType) string {
	switch queueType {
	case workloadmeta.KueueLocalQueue:
		return kubernetes.KueueLocalQueueResourceName + "." + kubernetes.KueueGroupName
	case workloadmeta.KueueClusterQueue:
		return kubernetes.KueueClusterQueueResourceName + "." + kubernetes.KueueGroupName
	default:
		return ""
	}
}
