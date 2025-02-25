<template>
    <h1 class="title is-1">Restore Files</h1>
    <div class="columns">
        <div class="column is-one-quarter">
            <h2 class="subtitle is-2">Snapshots</h2>
            <b>(Select one)</b>
            <table class="table">
                <tbody>
                    <tr>
                        <th>ID</th>
                        <th>Path</th>
                        <th>Timestamp</th>
                    </tr>
                    <tr
                        v-for="snapshot in snapshots"
                        :key="snapshot.id"
                        class="is-selectable"
                        :class="{ 'is-selected': snapshot.id === activeSnapshot?.id }"
                        @click="activeSnapshot = snapshot"
                    >
                        <td>{{ snapshot.id }}</td>
                        <td>
                            {{ snapshot.path }}
                        </td>
                        <td>
                            {{ new Date(snapshot.timestamp).toLocaleString() }}
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
        <div class="column">
            <h2 class="subtitle is-2">Files</h2>
            <ul v-if="activeSnapshot && rootName && rootObj && rootId">
                <FileList
                    :obj="rootObj"
                    :name="rootName"
                    :id="rootId"
                    @restore="showRestoreModal($event)"
                />
            </ul>
            <div v-else>Choose a snapshot</div>
        </div>
        <StyledModal v-model="modalShow">
            <div class="card">
                <header class="card-header">
                    <p class="card-header-title">
                        <span class="title is-2">Restore File</span>
                    </p>
                </header>
                <div class="card-content">
                    <div class="content">
                        <table class="table">
                            <tbody>
                                <tr>
                                    <th>Name</th>
                                    <td>{{ restoreInfo?.name }}</td>
                                </tr>
                                <tr>
                                    <th>Type</th>
                                    <td v-if="restoreInfo?.obj?.type === 'file'">File</td>
                                    <td v-else-if="restoreInfo?.obj?.type === 'tree'">
                                        Directory
                                    </td>
                                    <td v-else-if="restoreInfo?.obj?.type === 'symlink'">
                                        Symbolic Link
                                    </td>
                                    <td v-else>Other</td>
                                </tr>
                                <tr>
                                    <th>Size</th>
                                    <td>
                                        {{
                                            restoreInfo?.obj?.type === "file"
                                                ? restoreInfo?.obj?.file_size
                                                : "-"
                                        }}
                                    </td>
                                </tr>
                                <tr>
                                    <th>Last Modified</th>
                                    <td>
                                        {{
                                            restoreInfo?.obj?.last_modified_time
                                                ? new Date(
                                                      restoreInfo.obj.last_modified_time,
                                                  ).toLocaleString()
                                                : ""
                                        }}
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                        <div class="field is-grouped is-grouped-right">
                            <div class="control">
                                <button class="button is-link is-outlined">Cancel</button>
                            </div>
                            <div class="is-flex-grow-1"></div>
                            <div class="control">
                                <button class="button is-link">Restore</button>
                            </div>
                            <div class="control">
                                <button class="button is-link is-light">Download</button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </StyledModal>
    </div>
</template>

<!--suppress CssUnresolvedCustomProperty -->
<style scoped>
.table tbody tr.is-selectable {
    cursor: pointer;
}
.table tbody tr.is-selectable:not(.is-selected):hover {
    background-color: var(--table-row-hover-background-color);
}
.table tbody tr.is-selected {
    background-color: var(--table-row-active-background-color);
    color: var(--table-row-active-color);
}
</style>

<script setup lang="ts">
import { type components } from "@/schema";
import { conditionalUseQuery, useQuery } from "@/api.ts";
import { computed, type Ref, ref, toRefs } from "vue";
import FileList, { type FileListProps } from "@/components/FileList.vue";
import StyledModal from "@/components/StyledModal.vue";

const { data: snapshots } = toRefs(useQuery("get", "/snapshots", {}));

const activeSnapshot: Ref<components["schemas"]["Snapshot"] | null> = ref(null);

const restoreInfo = ref<FileListProps | null>(null);
const modalShow = ref<boolean>(false);

function showRestoreModal(objinfo: FileListProps) {
    restoreInfo.value = objinfo;
    modalShow.value = true;
}

const { data: rootObj } = toRefs(
    conditionalUseQuery(() => {
        if (activeSnapshot.value) {
            return useQuery("get", "/objects/{objid}", {
                params: {
                    path: {
                        objid: activeSnapshot.value.root,
                    },
                },
            });
        }
    }),
);

const rootName = computed(() => {
    if (activeSnapshot.value) {
        const date = new Date(activeSnapshot.value.timestamp);
        return `Snapshot-${date.toISOString()}`;
    }
    return null;
});

const rootId = computed(() => (rootName.value ? window.btoa(rootName.value) : null));
</script>
