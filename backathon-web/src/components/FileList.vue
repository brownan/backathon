<template>
    <li>
        <div class="filelist-directory">
            <button
                type="button"
                v-if="isTree"
                class="icon"
                @click="expanded = !expanded"
            >
                <MdiIcon
                    class="expand-icon"
                    :path="mdiMenuRight"
                    :rotate="expanded ? 90 : 0"
                />
            </button>
            <div
                v-else
                class="icon"
            >
                <!-- spacer, if no expand icon -->
            </div>
            <div
                class="icon"
                @click="isTree && (expanded = !expanded)"
            >
                <MdiIcon :path="isTree ? mdiFolderOutline : mdiFileOutline" />
            </div>
            <div
                class="filelist-name"
                @click="isTree && (expanded = !expanded)"
            >
                {{ name }}
            </div>
            <button
                type="button"
                class="button is-small"
            >
                Restore
            </button>
        </div>
        <ul
            v-if="expanded && isTree"
            class="filelist-children"
        >
            <FileList
                v-for="child in objList"
                :obj="child.obj"
                :name="child.name"
                :id="child.id"
                :key="child.id"
            />
        </ul>
    </li>
</template>

<style scoped>
.filelist-directory {
    display: flex;
    flex-wrap: wrap;
    column-gap: 1em;
    row-gap: 0.5em;
}
.filelist-directory:hover {
    background-color: var(--scheme-main-bis);
}
.filelist-children {
    margin-left: 16px;
}
.filelist-name {
    flex-grow: 1;
}
.expand-icon {
    transition: transform linear 0.2s;
}
</style>

<script setup lang="ts">
import { type components } from "@/schema";
import { computed, ref, toRefs } from "vue";
import { conditionalUseQuery, useQuery } from "@/api";

import MdiIcon from "@/utils/MdiIcon.vue";
import { mdiMenuRight, mdiFolderOutline, mdiFileOutline } from "@mdi/js";

/**
 * This component takes an object ID that's either a file or a directory, and
 * displays it.
 *
 * Props:
 * * objid - the object to display

 * Need some notion of a "path" to this object, because objects can appear in multiple
 * places in a tree. This also includes a name for this object, because an object doesn't
 * have a name of its own, just the name it's given by whatever directory it's in.
 *
 * This component should have the ability to select and un-select itself.
 * ... or does it? What if I just had a "download" link by each one? You could choose
 * to restore the entire tree, or just one file. Selecting a complex subset of the backup
 * would be more complicated, and maybe not even that useful. Unless you wanted to restore
 * /almost/ everything, but there's like one or two things that are real big and you don't
 * care about.
 *
 * Perhaps that's a later iteration.
 */

const props = defineProps<{
    obj: components["schemas"]["Object"];
    name: string; // Printable name
    id: string; // Actual name, base64 encoded
}>();

const isTree = computed(() => props.obj.type === "tree");

const { data: objList } = toRefs(
    conditionalUseQuery(() => {
        if (isTree.value) {
            return useQuery("get", "/objects/{objid}/ls", {
                params: {
                    path: {
                        objid: props.obj.objid,
                    },
                },
            });
        }
    }),
);

const expanded = ref<boolean>(false);
</script>
