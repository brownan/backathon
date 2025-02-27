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
                @click="() => showRestoreModal()"
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
                :path-prefix="`${props.pathPrefix}/${child.name}`"
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
import { computed, reactive, ref, toRefs, watch } from "vue";
import { conditionalUseQuery, useQuery } from "@/api";

import MdiIcon from "@/utils/MdiIcon.vue";
import { mdiMenuRight, mdiFolderOutline, mdiFileOutline } from "@mdi/js";
import { useModal } from "vue-final-modal";
import RestoreModal from "@/components/RestoreModal.vue";
import { useSessionStorage, StorageSerializers } from "@vueuse/core";

/**
 * This component takes an object ID that's either a file or a directory, and
 * displays it.
 */

export type FileListProps = {
    obj: components["schemas"]["Object"];
    name: string; // Printable name
    id: string; // Actual name, base64 encoded
    pathPrefix: string;
};

const props = defineProps<FileListProps>();
const { obj, name } = toRefs(props);

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

const { open: showRestoreModal } = useModal({
    component: RestoreModal,
    attrs: reactive({
        obj,
        name,
    }),
});

const sessionStorage = useSessionStorage<Set<string>>(
    "restore-file-list-expanded",
    new Set(),
    {
        serializer: StorageSerializers.set,
    },
);

const expanded = ref<boolean>(sessionStorage.value.has(props.pathPrefix));
watch(expanded, (newval) => {
    if (!newval) {
        sessionStorage.value.delete(props.pathPrefix);
    } else {
        sessionStorage.value.add(props.pathPrefix);
    }
});
</script>
