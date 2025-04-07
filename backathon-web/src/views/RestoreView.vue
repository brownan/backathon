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
            <h2 class="subtitle is-2">Files</h2>
            <ul v-if="activeSnapshot && rootName && rootObj && rootId">
                <FileList
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
import { computed, toRefs } from "vue";
import FileList from "@/components/FileList.vue";
import { useRoute, useRouter } from "vue-router";

const snapshotResult = useQuery({
    method: "get",
    url: "/snapshots",
    options: {},
});
const snapshots = computed(() => snapshotResult.data || []);

const routes = useRoute();
const router = useRouter();

const activeSnapshot = computed(() => {
    if (!snapshots.value) {
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
    return null;
});

function selectSnapshot(id: number) {
    router.replace({ name: "restore", params: { id } });
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
        const date = new Date(activeSnapshot.value.timestamp);
        return `Snapshot-${date.toISOString()}`;
    }
    return null;
});

const rootId = computed(() => (rootName.value ? window.btoa(rootName.value) : null));
</script>
