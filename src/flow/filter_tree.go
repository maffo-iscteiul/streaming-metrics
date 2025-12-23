package flow

import "github.com/itchyny/gojq"

/*
 * Filter root
 */

type Filter_root struct {
	group_filter *gojq.Code
	groups       map[string]*Group_node
}

func New_filter_tree(group_filter *gojq.Code) *Filter_root {
	return &Filter_root{
		group_filter: group_filter,
		groups:       make(map[string]*Group_node),
	}
}

func (r *Filter_root) Has_group(group string) bool {
	_, ok := r.groups[group]
	return ok
}

func (r *Filter_root) Add_group(group string, node *Group_node) {
	r.groups[group] = node
}

func (r *Filter_root) Get_group(group string) *Group_node {
	return r.groups[group]
}

/*
 * Group_node
 */

type Group_node struct {
	name string
	//group_filter *gojq.Code
	children []*Leaf_node
}

func New_group_node(name string /*, group_filter *gojq.Code*/) *Group_node {
	return &Group_node{
		name: name,
		//group_filter: group_filter,
		children: make([]*Leaf_node, 0),
	}
}

func (gf *Group_node) Add_child(leaf *Leaf_node) {
	gf.children = append(gf.children, leaf)
}

/*
 * Leaf_node
 */

type Leaf_node struct {
	Filter *gojq.Code
}
