<template>
    <h2 class="title is-4">{{ snapshot.path }}</h2>
    <ul>
        <li>Current dir: {{ currentDir }}</li>
        <li>parent dir: {{ parentDir }}</li>
    </ul>
    <table class="table is-striped is-fullwidth">
        <thead>
            <tr>
                <th></th>
                <th>Name</th>
                <th>Size</th>
                <th>Last Modified</th>
            </tr>
        </thead>
        <tbody>
            <tr
                v-for="file in files"
                :key="file.name"
            >
                <td>
                    <input
                        type="checkbox"
                        :checked="selectedObjects.has(file.objid)"
                        @change="
                            selectedObjects.has(file.objid)
                                ? selectedObjects.delete(file.objid)
                                : selectedObjects.add(file.objid)
                        "
                    />
                </td>
                <td>{{ file.name }}{{ file.type === "tree" ? "/" : "" }}</td>
                <td>{{ file.size }}</td>
                <td>{{ file.lastModified?.toLocaleString() }}</td>
            </tr>
        </tbody>
    </table>
</template>

<style scoped></style>

<script setup lang="ts">
import { type components } from "@/schema";
import { computed, reactive } from "vue";
import { useQuery } from "@/api";

const props = defineProps<{
    snapshot: components["schemas"]["Snapshot"];
}>();

const { data: rootObj } = useQuery("get", "/objects/{objid}", () => {
    return {
        params: {
            path: {
                objid: props.snapshot.root,
            },
        },
    };
});

type ObjectWithName = components["schemas"]["Object"] & { name: string };

const path = reactive([] as ObjectWithName[]);

const currentObj = computed(() =>
    path.length > 0
        ? path[path.length - 1]
        : rootObj.value
        ? { name: props.snapshot.path, ...rootObj.value }
        : null,
);

const selectedObjects = reactive(new Set() as Set<string>);

const { data: currentDirList } = useQuery("get", "/objects/{objid}/ls", () => {
    return {
        params: {
            path: {
                objid: currentObj.value?.objid,
            },
        },
    };
});

const files = computed(() => {
    return currentDirList.value?.map(([name, obj]) => {
        return {
            name: name,
            objid: obj.objid,
            type: obj.type,
            size: obj.file_size,
            lastModified: obj.last_modified_time
                ? new Date(obj.last_modified_time)
                : null,
        };
    });
});

// get info about this snapshot, including the root object
</script>
