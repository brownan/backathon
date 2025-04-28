<template>
    <h1 class="title is-2">Restore Files</h1>
    <div class="columns">
        <div class="column is-one-third">
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
                        @click="selectSnapshot(snapshot.id)"
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
            <div class="panel">
                <div class="panel-heading">Snapshot Info</div>
                <div class="panel-block is-flex">
                    <table class="table is-narrow">
                        <tbody>
                            <tr>
                                <th>Path</th>
                                <td>
                                    <SpinnerIcon v-if="snapshotInfo.isFetching" />
                                    <template v-else-if="snapshotInfo.data">
                                        {{ snapshotInfo.data.path }}
                                    </template>
                                </td>
                            </tr>
                            <tr>
                                <th>Timestamp</th>
                                <td>
                                    <SpinnerIcon v-if="snapshotInfo.isFetching" />
                                    <template v-else-if="snapshotInfo.data">
                                        {{
                                            new Date(
                                                snapshotInfo.data.timestamp,
                                            ).toLocaleString()
                                        }}
                                    </template>
                                </td>
                            </tr>
                            <tr>
                                <th>Total size of all files</th>
                                <td>
                                    <SpinnerIcon v-if="snapshotExtendedInfo.isFetching" />
                                    <template v-else-if="snapshotExtendedInfo.data">
                                        {{ filesize(snapshotExtendedInfo.data.fileSize) }}
                                    </template>
                                </td>
                            </tr>
                            <tr>
                                <th>Uploaded Size</th>
                                <td>
                                    <SpinnerIcon v-if="snapshotExtendedInfo.isFetching" />
                                    <template v-else-if="snapshotExtendedInfo.data">
                                        {{
                                            filesize(
                                                snapshotExtendedInfo.data.uploadedSize,
                                            )
                                        }}
                                    </template>
                                </td>
                            </tr>
                            <tr>
                                <th>Exclusive Size</th>
                                <td>
                                    <SpinnerIcon v-if="snapshotExtendedInfo.isFetching" />
                                    <template v-else-if="snapshotExtendedInfo.data">
                                        {{
                                            filesize(
                                                snapshotExtendedInfo.data.exclusiveSize,
                                            )
                                        }}
                                        (approx.)
                                    </template>
                                </td>
                            </tr>
                            <tr>
                                <th>Shared Size</th>
                                <td>
                                    <SpinnerIcon v-if="snapshotExtendedInfo.isFetching" />
                                    <template v-else-if="snapshotExtendedInfo.data">
                                        {{
                                            filesize(snapshotExtendedInfo.data.sharedSize)
                                        }}
                                        (approx.)
                                    </template>
                                </td>
                            </tr>
                        </tbody>
                    </table>
                    <div class="is-flex-grow-1"></div>
                    <div class="is-align-self-flex-start">
                        <button
                            type="button"
                            class="button is-danger"
                            @click="showDeleteSnapshotModal"
                        >
                            Delete
                        </button>
                    </div>
                </div>
            </div>
            <h2 class="subtitle is-2">Files</h2>
            <ul v-if="activeSnapshot && rootName && rootObj && rootId">
                <RestoreTree
                    :obj="rootObj"
                    :name="rootName"
                    :id="rootId"
                    :path-prefix="activeSnapshot.path"
                />
            </ul>
            <div v-else>Choose a snapshot</div>
        </div>
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
import { useQuery } from "@/api.ts";
import { computed, reactive, toRef, toRefs } from "vue";
import RestoreTree from "@/components/RestoreTree.vue";
import { useRoute, useRouter } from "vue-router";
import SpinnerIcon from "@/components/SpinnerIcon.vue";
import { filesize } from "filesize";
import { useModal } from "vue-final-modal";
import DeleteSnapshot from "@/components/DeleteSnapshot.vue";

const snapshotResult = useQuery({
    method: "get",
    url: "/snapshots",
    options: {},
});
const snapshots = computed(() => snapshotResult.data || []);

const routes = useRoute();
const router = useRouter();

const activeSnapshot = computed(() => {
    if (!routes.params.id) {
        return null;
    }
    if (!snapshots.value || snapshots.value.length === 0) {
        return null;
    }
    let targetId;
    try {
        targetId = Number.parseInt(routes.params.id as string);
    } catch {
        return null;
    }
    for (const snapshot of snapshots.value) {
        if (snapshot.id === targetId) {
            return snapshot;
        }
    }
    selectSnapshot(null);
    return null;
});

function selectSnapshot(id: number | null) {
    // check if this snapshot id actually exists
    if (id === null || !snapshots.value.some((s) => s.id === id)) {
        router.replace({ name: "restore", params: {} });
    } else {
        router.replace({ name: "restore", params: { id } });
    }
}

const { data: rootObj } = toRefs(
    useQuery(() => {
        if (activeSnapshot.value) {
            return {
                method: "get",
                url: "/objects/{objid}",
                options: {
                    params: {
                        path: {
                            objid: activeSnapshot.value.root,
                        },
                    },
                },
            };
        }
    }),
);

const rootName = computed(() => {
    if (activeSnapshot.value) {
        return `${activeSnapshot.value.path}`;
    }
    return null;
});

const rootId = computed(() => (rootName.value ? window.btoa(rootName.value) : null));

const snapshotInfo = useQuery(() => {
    if (activeSnapshot.value) {
        return {
            method: "get",
            url: "/snapshots/{id}",
            options: {
                params: {
                    path: {
                        id: activeSnapshot.value.id,
                    },
                },
            },
        };
    }
});

const snapshotExtendedInfo = useQuery(() => {
    if (activeSnapshot.value) {
        return {
            method: "get",
            url: "/snapshots/{id}/extended",
            options: {
                params: {
                    path: {
                        id: activeSnapshot.value.id,
                    },
                },
            },
        };
    }
});

const { open: showDeleteSnapshotModal } = useModal({
    component: DeleteSnapshot,
    attrs: reactive({
        snapshotInfo: toRef(snapshotInfo, "data"),
        snapshotExtendedInfo: toRef(snapshotExtendedInfo, "data"),
    }),
});
</script>
