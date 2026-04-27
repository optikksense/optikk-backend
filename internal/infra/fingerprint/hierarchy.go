package fingerprint

// DimensionHierarchyNode represents one level in the resource identity tree.
// Each node holds a set of synonymous attribute keys (e.g., "service.name" and
// "k8s.deployment.name" both identify the service). The walker picks the first
// key that exists in the resource attributes and then recurses into child
// hierarchies.
type DimensionHierarchyNode struct {
	// labels are synonymous attribute keys at this level.
	// The first one found in the resource wins.
	labels []string

	// subHierarchies are child nodes tried in order. Multiple entries
	// represent alternative grouping views (e.g., service-oriented vs
	// node-oriented).
	subHierarchies []DimensionHierarchyNode
}

// IdLabelValue is a matched (key, value) pair from the hierarchy walk.
type IdLabelValue struct {
	Label string
	Value string
}

// Identifier walks the hierarchy tree against the given attributes and returns
// the list of matched (label, value) pairs that form the identity prefix.
func (n *DimensionHierarchyNode) Identifier(attrs map[string]string) []IdLabelValue {
	var result []IdLabelValue

	// At this level, pick the first label that exists in attrs
	for _, l := range n.labels {
		if v, ok := attrs[l]; ok && v != "" {
			result = append(result, IdLabelValue{Label: l, Value: v})
			break
		}
	}

	// Recurse into the first sub-hierarchy that produces matches
	for _, sub := range n.subHierarchies {
		subLabels := sub.Identifier(attrs)
		if len(subLabels) > 0 {
			result = append(result, subLabels...)
			break
		}
	}

	return result
}

// ResourceHierarchy returns the standard OpenTelemetry resource identity tree.
// Attributes are ordered from broadest (cloud provider) to most specific
// (container name). This matches the SigNoz dimension hierarchy.
func ResourceHierarchy() *DimensionHierarchyNode {
	return &DimensionHierarchyNode{
		labels: []string{"cloud.provider"},
		subHierarchies: []DimensionHierarchyNode{{
			labels: []string{"cloud.account.id"},
			subHierarchies: []DimensionHierarchyNode{{
				labels: []string{"gcp.project"},
				subHierarchies: []DimensionHierarchyNode{{
					labels: []string{
						"cloud.region",
						"aws.region",
					},
					subHierarchies: []DimensionHierarchyNode{{
						labels: []string{
							"cloud.platform",
							"source_type",
						},
						subHierarchies: []DimensionHierarchyNode{{
							labels: []string{
								"k8s.cluster.name",
								"k8s.cluster.uid",
								"aws.ecs.cluster.arn",
							},
							subHierarchies: []DimensionHierarchyNode{
								// Service-oriented view (tried first)
								{
									labels: []string{
										"service.namespace",
										"k8s.namespace.name",
									},
									subHierarchies: []DimensionHierarchyNode{{
										labels: []string{
											"service.name",
											"cloudwatch.log.group.name",
											"k8s.deployment.name",
											"k8s.deployment.uid",
											"k8s.statefulset.name",
											"k8s.statefulset.uid",
											"k8s.daemonset.name",
											"k8s.daemonset.uid",
											"k8s.job.name",
											"k8s.job.uid",
											"k8s.cronjob.name",
											"k8s.cronjob.uid",
											"faas.name",
										},
										subHierarchies: []DimensionHierarchyNode{{
											labels: []string{
												"deployment.environment",
												"env",
											},
											subHierarchies: []DimensionHierarchyNode{{
												labels: []string{
													"service.version",
													"version",
												},
												subHierarchies: []DimensionHierarchyNode{{
													labels: []string{
														"service.instance.id",
														"k8s.pod.name",
														"k8s.pod.uid",
														"aws.ecs.task.id",
														"aws.ecs.task.arn",
														"cloudwatch.log.stream",
														"cloud.resource_id",
														"faas.instance",
														"host.id",
														"host.name",
														"host.ip",
														"host",
													},
													subHierarchies: []DimensionHierarchyNode{{
														labels: []string{
															"k8s.container.name",
															"container.name",
															"container_name",
														},
														subHierarchies: []DimensionHierarchyNode{{
															labels: []string{"component"},
														}},
													}},
												}},
											}},
										}},
									}},
								},
								// Node-oriented view (fallback)
								{
									labels: []string{"cloud.availability_zone"},
									subHierarchies: []DimensionHierarchyNode{{
										labels: []string{
											"k8s.node.name",
											"k8s.node.uid",
											"host.id",
											"host.name",
											"host.ip",
											"host",
										},
										subHierarchies: []DimensionHierarchyNode{{
											labels: []string{
												"k8s.pod.name",
												"k8s.pod.uid",
												"aws.ecs.task.id",
												"aws.ecs.task.arn",
											},
											subHierarchies: []DimensionHierarchyNode{{
												labels: []string{
													"k8s.container.name",
													"container.name",
												},
											}},
										}},
									}},
								},
							},
						}},
					}},
				}},
			}},
		}},
	}
}
