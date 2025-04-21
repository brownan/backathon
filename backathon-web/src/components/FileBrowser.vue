<template>
    <div>
        <label>
            <Checkbox
                v-model="showHiddenFiles"
                binary
            />
            Show Hidden Files
        </label>
        <Tree
            :class="{ 'hide-hidden-files': !showHiddenFiles }"
            :selection-keys="selectedKeys"
            v-model:expanded-keys="expandedKeys"
            :value="nodes"
            @node-expand="onNodeExpand"
            @node-collapse="onNodeCollapse"
            :pt="{
                wrapper: 'file-browser-wrapper',
                rootChildren: 'file-browser-root-children',
                nodeChildren: 'file-browser-node-children',
                nodeContent: 'file-browser-content',
                node: ({ context }) => ['file-browser-node', getNodeClass(context.node)],
                nodeToggleButton: 'file-browser-toggle-button',
                nodeLabel: ({ context }) => [
                    'nodelabel',
                    getNodeLabelClass(context.node),
                ],
            }"
        >
            <template v-slot:nodeicon="{ node }">
                <Checkbox @click="onCheckClick(node)">
                    <template v-slot:icon>
                        <CheckIcon v-if="selectedKeys[node.key]?.checked" />
                        <MinusIcon v-else-if="selectedKeys[node.key]?.partialChecked" />
                        <TimesIcon v-else-if="selectedKeys[node.key]?.excluded" />
                    </template>
                </Checkbox>
            </template>
            <template v-slot:default="{ node }">
                {{ node.label }}
                <span
                    class="root-text"
                    v-if="getNodeInfo(node)?.root"
                    >(root)</span
                >
                <span
                    class="excluded-text"
                    v-if="getNodeInfo(node)?.excluded"
                    >(excluded)</span
                >
            </template>
        </Tree>
    </div>
</template>

<style>
.nodelabel.excluded {
    text-decoration: line-through;
}

.nodelabel .root-text {
    font-weight: bold;
    color: red;
}

.nodelabel .excluded-text {
    font-weight: bold;
    color: red;
}
.file-browser-wrapper {
}

.file-browser-root-children,
.file-browser-node-children {
    display: flex;
    list-style-type: none;
    flex-direction: column;
    margin: 0;
    gap: 2px;
}
.file-browser-root-children {
    padding: 0;
    padding-block-start: 2px;
}
.file-browser-node-children {
    padding: 0;
    padding-block-start: 2px;
    padding-inline-start: 2rem;
}
.file-browser-node {
    padding: 0;
    outline: 0 none;
}

.hide-hidden-files .file-browser-node.node-hidden {
    display: none;
}

.file-browser-content {
    border-radius: 2px;
    padding: 0 0.5rem;
    display: flex;
    align-items: center;
    outline-color: transparent;
    color: var(--text);
    gap: 0.5rem;
    transition:
        background 1s,
        color 1s,
        outline-color 1s,
        box-shadow 1s;
}
.file-browser-toggle-button {
    cursor: pointer;
    user-select: none;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    overflow: hidden;
    position: relative;
    flex-shrink: 0;
    width: 2rem;
    height: 1.5rem;
    color: var(--text-weak);
    border: 0 none;
    background: transparent;
    border-radius: 50%;
    transition:
        background 1s,
        color 1s,
        border-color 1s,
        outline-color 1s,
        box-shadow 1s;
    outline-color: transparent;
    padding: 0;
}
</style>

<script setup lang="ts">
/**
 * File Browser component
 */
import { reactive, ref } from "vue";
import Tree from "primevue/tree";
import { type TreeNode } from "primevue/treenode";
import { client } from "@/api.ts";
import { type components } from "@/schema";

import Checkbox from "primevue/checkbox";
import CheckIcon from "@primevue/icons/check";
import MinusIcon from "@primevue/icons/minus";
import TimesIcon from "@primevue/icons/times";

const showHiddenFiles = ref<boolean>(false);

// Passed to the Tree component to define the tree nodes
const nodes = reactive<TreeNode[]>([
    {
        key: "Lw==",
        label: "/",
    },
]);

// Passed to the Tree component to define the checked status
const selectedKeys = ref<{
    [key: string]:
        | { checked?: boolean; partialChecked?: boolean; excluded?: boolean }
        | undefined;
}>({});

function getNodeClass(node: TreeNode) {
    if (!node.label) {
        return;
    }
    const labelParts = node.label?.split("/");
    const finalPart = labelParts[labelParts.length - 1];
    return finalPart.startsWith(".") ? "node-hidden" : null;
}

function getNodeLabelClass(node: TreeNode) {
    if (selectedKeys.value[node.key]?.excluded) {
        return "excluded";
    }
}

/**
 * Checks this node's ancestors to find what "child flags" should be set
 *
 * childOfRoot indicates this node is implicitly included in a backup set
 * childOfExclude indicates this node is implicitly excluded, even though a further
 *   ancestor may be a root
 *
 * These flags are use to determine what kind of checkbox to use for a node, and also
 * what to do when a checkbox is clicked
 */
function getNodeChildFlags(node: TreeNode | null): {
    childOfRoot: boolean;
    childOfExclude: boolean;
} {
    if (!node) {
        return { childOfRoot: false, childOfExclude: false };
    }
    while ((node = getNodeParent(node))) {
        const nodeInfo = getNodeInfo(node);
        if (nodeInfo?.root) {
            return { childOfRoot: true, childOfExclude: false };
        } else if (nodeInfo?.excluded) {
            return { childOfRoot: false, childOfExclude: true };
        }
    }
    return { childOfRoot: false, childOfExclude: false };
}

// Updates the checked status of the given node
function updateCheckedStatus(node: TreeNode) {
    const nodeInfo = getNodeInfo(node);
    if (!nodeInfo) {
        delete selectedKeys.value[node.key];
        return;
    }

    const isRoot = nodeInfo.root;
    const isExcluded = nodeInfo.excluded;
    const parentOfRoot = nodeInfo.parentOfRoot;
    const { childOfRoot, childOfExclude } = getNodeChildFlags(node);

    if (isRoot) {
        // This is a root node
        selectedKeys.value[node.key] = { checked: true };
    } else if (isExcluded) {
        // This node is explicitly excluded
        selectedKeys.value[node.key] = { excluded: true };
    } else if (parentOfRoot) {
        // This is the parent of some root, so we indicate this with
        // a partial checkmark
        selectedKeys.value[node.key] = { partialChecked: true };
    } else if (childOfRoot) {
        // A child of a root is implicitly included and indicated with a check
        selectedKeys.value[node.key] = { checked: true };
    } else if (childOfExclude) {
        // A child of an exclude is implicitly excluded, and indicated with the excluded
        // icon
        selectedKeys.value[node.key] = { excluded: true };
    } else {
        delete selectedKeys.value[node.key];
    }
}

function fetchDirContents(node: TreeNode) {
    client
        .GET("/browse/{key}", {
            params: {
                path: {
                    key: node.key,
                },
            },
        })
        .then((result) => {
            if (result.data) {
                node.children = result.data.children.map((child) => ({
                    key: child.key,
                    label: child.path,
                    nodeInfo: child,
                    parent: node,
                }));
                for (const child of node.children) {
                    updateCheckedStatus(child);
                }

                node.nodeInfo = result.data.info;
                updateCheckedStatus(node);
            }
        });
}

fetchDirContents(nodes[0]);

// This function adds typing info for use in templates and elsewhere
function getNodeInfo(node: TreeNode): components["schemas"]["PathInfo"] | null {
    return node?.nodeInfo || null;
}

function getNodeParent(node: TreeNode): TreeNode | null {
    return node.parent || null;
}

const expandedKeys = ref<{ [key: string]: boolean }>({ "Lw==": true });
function onNodeExpand(node: TreeNode) {
    console.log("on node expand", node);
    fetchDirContents(node);
}

function onNodeCollapse(node: TreeNode) {
    // Cull the tree in memory so things don't grow as the user
    // opens and closes directories
    if (!node.children) {
        return;
    }
    for (const child of node.children) {
        onNodeCollapse(child);
    }
    node.children = [];
}

function onCheckClick(node: TreeNode) {
    console.log("Check clicked", node);
    const nodeInfo = getNodeInfo(node);

    if (!nodeInfo) {
        console.warn("No node info found");
        return;
    }

    const isRoot = nodeInfo.root;
    const isExcluded = nodeInfo.excluded;
    const { childOfRoot, childOfExclude } = getNodeChildFlags(node);

    if (isRoot) {
        console.log("Root clicked. removing root");
        nodeInfo.root = false;
        setCheckStatusRemovePartial(node);
        updateDescendentCheckStatus(node);

        client.DELETE("/roots/{key}", {
            params: {
                path: {
                    key: node.key,
                },
            },
        });
    } else if (isExcluded) {
        console.log("Exclude clicked. Removing exclude");
        nodeInfo.excluded = false;
        updateDescendentCheckStatus(node);

        client.DELETE("/excludes/{key}", {
            params: {
                path: {
                    key: node.key,
                },
            },
        });
    } else if (childOfRoot) {
        console.log("Implicit include clicked. Adding exclude");
        nodeInfo.excluded = true;
        updateDescendentCheckStatus(node);

        client.PUT("/excludes/{key}", {
            params: {
                path: {
                    key: node.key,
                },
            },
        });
    } else if (childOfExclude) {
        console.log("Implicit exclude clicked. No action");
    } else {
        console.log("Unchecked or partially checked node clicked. Adding root");
        nodeInfo.root = true;
        setCheckStatusPartial(node);
        updateDescendentCheckStatus(node);

        client.PUT("/roots/{key}", {
            params: {
                path: {
                    key: node.key,
                },
            },
        });
    }
}

/*
 * Recurses downward and updates the checked status of all nodes. Generally
 * called on any node that's had its root or excluded flag changed, so that
 * descendents can update the UI accordingly.
 */
function updateDescendentCheckStatus(start: TreeNode) {
    updateCheckedStatus(start);
    if (start.children) {
        for (const child of start.children) {
            updateDescendentCheckStatus(child);
        }
    }
}

/*
 * Called on new roots. Goes and sets the parentOfRoot flag on all ancestors
 */
function setCheckStatusPartial(node: TreeNode) {
    if (node.parent) {
        const parent = node.parent;
        const nodeInfo = getNodeInfo(parent);
        if (nodeInfo) {
            nodeInfo.parentOfRoot = true;
        }
        setCheckStatusPartial(parent);
        updateCheckedStatus(parent);
    }
}

/*
 * Called on newly removed roots. Goes and clears the parentOfRoot flag
 * on all ancestors, unless those ancestors have any children that are either
 * roots or themselves parents of roots.
 */
function setCheckStatusRemovePartial(node: TreeNode) {
    const parent = getNodeParent(node);
    if (parent) {
        const nodeInfo = getNodeInfo(parent);
        if (
            nodeInfo?.parentOfRoot &&
            parent.children &&
            !parent.children.some(
                (child) => getNodeInfo(child)?.root || getNodeInfo(child)?.parentOfRoot,
            )
        ) {
            nodeInfo.parentOfRoot = false;
            setCheckStatusRemovePartial(parent);
            updateCheckedStatus(parent);
        }
    }
}
</script>
