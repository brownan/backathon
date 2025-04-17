<template>
    <div>
        <Tree
            v-model:selection-keys="selectedKeys"
            v-model:expanded-keys="expandedKeys"
            :value="nodes"
            selection-mode="checkbox"
            @node-expand="onNodeExpand"
            :pt="{
                wrapper: 'file-browser-wrapper',
                rootChildren: 'file-browser-root-children',
                nodeChildren: 'file-browser-node-children',
                nodeContent: 'file-browser-content',
                node: 'file-browser-node',
                nodeToggleButton: 'file-browser-toggle-button',
            }"
        />
    </div>
</template>

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
import { reactive } from "vue";
import Tree from "primevue/tree";
import { type TreeNode } from "primevue/treenode";
import { client } from "@/api.ts";

const nodes = reactive<TreeNode[]>([
    {
        key: "Lw==",
        label: "/",
    },
]);

function fetchDirContents(node: TreeNode) {
    client
        .GET("/roots/browse", {
            params: {
                query: {
                    path_b64: node.key,
                },
            },
        })
        .then((result) => {
            node.children =
                result?.data?.map(({ path_str, path_b64 }) => ({
                    key: path_b64,
                    label: path_str,
                })) || [];
        });
}

fetchDirContents(nodes[0]);

function onNodeExpand(node: TreeNode) {
    console.log("on node expand", node);
    fetchDirContents(node);
}

const expandedKeys = reactive<{ [key: string]: boolean }>({ "Lw==": true });

const selectedKeys = reactive<{
    [key: string]: { checked?: boolean; partialChecked?: boolean };
}>({
    a: { checked: true },
});
</script>
