<template>
    <div>
        <Tree
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
                node: 'file-browser-node',
                nodeToggleButton: 'file-browser-toggle-button',
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
                    class="root"
                    v-if="getNodeInfo(node)?.root"
                    >(root)</span
                >
                <span
                    class="excluded"
                    v-if="getNodeInfo(node)?.excluded"
                    >(excluded)</span
                >
            </template>
        </Tree>
    </div>
</template>

<style scoped>
.root {
    font-weight: bold;
    color: red;
}
.excluded {
    font-weight: bold;
    color: blue;
}
</style>

<style>
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
 *
 * Clicks:
 * - Unchecked or partial checked node is clicked
 *     Check it and all descendents up to any excluded nodes
 *     Partial check all ancestors
 *     Add node as a root
 * - Checked node is clicked:
 *     If root:
 *       Uncheck it
 *       Recurse downward unchecking nodes and removing explicit root flag. Stop recursing
 *         if an excluded node is hit
 *       Recurse upward and remove partial checks if none of its children are checked.
 *       Remove node as root
 *     If node is a child of a root:
 *       set checked status to "exclude"
 *       recurse downward setting everything to exclude
 *       Add to excludes
 * - Excluded node is clicked:
 *     If node is explicitly excluded:
 *       Remove exclude check
 *       Recurse downward and remove exclude checks from descendents
 *       Remove from excludes
 *     Otherwise: do nothing
 *
 * APIs:
 * - Get dir listing
 *     - Retrieves info on all directories that are an immediate child of given dir
 *     - Info includes root status, exclude status, parent of root (partial check),
 *       and ancestry distance to nearest parent and exclude (for check / exclude UI
 *       when this node is a descendent of a root or exclude)
 * - Add a root
 *     - Adds node as root
 * - Remove root
 *     - Removes node from root list
 * - Add exclude
 *     - Adds node to exclude list
 * - Remove exclude
 *     - Removes node from exclude list
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

// Updates the checked status of the given node
function updateCheckedStatus(node: TreeNode) {
    const nodeInfo = getNodeInfo(node);
    if (!nodeInfo) {
        delete selectedKeys.value[node.key];
        return;
    }
    if (nodeInfo.root) {
        // This is a root node
        selectedKeys.value[node.key] = { checked: true };
    } else if (nodeInfo.excluded) {
        // This node is explicitly excluded
        selectedKeys.value[node.key] = {};
    } else if (nodeInfo.parentOfRoot) {
        // This is the parent of some root, so we indicate this with
        // a partial checkmark
        selectedKeys.value[node.key] = { partialChecked: true };
    } else if (nodeInfo.childOfRoot) {
        // This node is the child of a root, so it is implicitly included
        // in the backup set
        selectedKeys.value[node.key] = { checked: true };
    }
}

function fetchDirContents(node: TreeNode) {
    client
        .GET("/roots/browse", {
            params: {
                query: {
                    key: node.key,
                },
            },
        })
        .then((result) => {
            if (result.data) {
                node.children = result.data.map((child) => ({
                    key: child.key,
                    label: child.path,
                    nodeInfo: child,
                    parent: node,
                }));
                for (const child of node.children) {
                    updateCheckedStatus(child);
                }
            }
        });
}

fetchDirContents(nodes[0]);

// This function adds typing info for use in templates and elsewhere
function getNodeInfo(node: TreeNode): components["schemas"]["RootBrowseReturn"] | null {
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
    if (selectedKeys.value[node.key]?.checked) {
        if (nodeInfo?.root) {
            console.log("Removing root. unchecking node", node);
            setCheckStatusUnchecked(node);
            // TODO: call remove-root api
        } else if (nodeInfo?.childOfRoot) {
            console.log("adding exclude. excluding node", node);
            setCheckStatusExcluded(node);
            // TODO: call add-exclude api
        }
    } else if (selectedKeys.value[node.key]?.excluded) {
        console.log("removing exclude. un-excluding node", node);
        setCheckStatusUnchecked(node);
        // TODO: call remove-exclude api
    } else {
        // unchecked or partial-checked case
        console.log("Adding root. Checking node", node);
        setCheckStatusChecked(node);
        if (node.parent) {
            setCheckStatusPartial(node.parent);
        }
        // TODO: call add-root api
    }
}

/*
 * Checks a node and all descendents up to any excluded nodes
 */
function setCheckStatusChecked(node: TreeNode) {
    selectedKeys.value[node.key] = { checked: true };
    if (node.children) {
        for (const child of node.children) {
            if (!selectedKeys.value[child.key]?.excluded) {
                setCheckStatusChecked(child);
            }
        }
    }
}

/*
 * Recurse downward unchecking nodes and removing any explicit root flag.
 * Stop recursing if an excluded node is hit
 */
function setCheckStatusUnchecked(node: TreeNode) {
    if (!getNodeInfo(node)?.excluded) {
        delete selectedKeys.value[node.key];
        const nodeInfo = getNodeInfo(node);
        if (nodeInfo?.root) {
            nodeInfo.root = false;
        }
        if (node.children) {
            for (const child of node.children) {
                setCheckStatusUnchecked(child);
            }
        }
    }
}

/*
 * Recurse downward and set everything to excluded
 */
function setCheckStatusExcluded(node: TreeNode) {
    selectedKeys.value[node.key] = { excluded: true };
    if (node.children) {
        for (const child of node.children) {
            setCheckStatusExcluded(child);
        }
    }
}

/*
 * Recurse downward and remove exclude checks from descendents
 * Stop if any explicitly excluded nodes are hit (they need to
 * be unexcluded explicitly)
 */
function setCheckStatusUnexcluded(node: TreeNode) {
    delete selectedKeys.value[node.key];
    if (node.children) {
        for (const child of node.children) {
            if (!getNodeInfo(child)?.excluded) {
                setCheckStatusUnexcluded(child);
            }
        }
    }
}

/*
 * Partial check all ancestors
 */
function setCheckStatusPartial(node: TreeNode) {
    selectedKeys.value[node.key] = { partialChecked: true };
    if (node.paren) {
        setCheckStatusPartial(node.parent);
    }
}

/*
 * Recurse upward and remove partial checks if none of its children are checked.
 */
function setCheckStatusRemovePartial(node: TreeNode) {
    if (selectedKeys.value[node.key]?.partialChecked) {
        if (
            node.children &&
            !node.children.some((child) => selectedKeys.value[child.key]?.checked)
        ) {
            delete selectedKeys.value[node.key];
            if (node.parent) {
                setCheckStatusRemovePartial(node.parent);
            }
        }
    }
}
</script>
