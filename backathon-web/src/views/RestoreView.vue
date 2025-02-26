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
import { type components } from "@/schema";
import { conditionalUseQuery, useQuery } from "@/api.ts";
import { computed, type Ref, ref, toRefs } from "vue";
import FileList from "@/components/FileList.vue";

const { data: snapshots } = toRefs(useQuery("get", "/snapshots", {}));

const activeSnapshot: Ref<components["schemas"]["Snapshot"] | null> = ref(null);

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
