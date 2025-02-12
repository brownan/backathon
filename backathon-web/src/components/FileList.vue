<template>
    <h2 class="title is-4">{{ snapshot.path }}</h2>
    <ul>
        <li>Current dir: {{ currentDir }}</li>
        <li>parent dir: {{ parentDir }}</li>
    </ul>
    <table class="table is-striped">
        <thead>
            <tr>
                <th>Name</th>
                <th>Size</th>
                <th>Last Modified</th>
            </tr>
        </thead>
        <tbody>
            <tr
                v-for="file in files"
                :key="file.id"
            >
                <td>{{ file.name }}</td>
                <td>{{ file.size }}</td>
                <td>{{ file.lastModified }}</td>
            </tr>
        </tbody>
    </table>
</template>

<style scoped></style>

<script setup lang="ts">
import { type components } from "@/schema";
import { computed, type Ref, ref } from "vue";
import { useQuery } from "@/api.ts";

const props = defineProps<{
    snapshot: components["schemas"]["Snapshot"];
}>();

// An object ID to a "tree" object
const currentDir: Ref<string> = ref(props.snapshot.root);
const parentDir: Ref<string | null> = ref(null);

const initParams = {
    params: {
        path: {
            objid: currentDir.value,
        },
    },
};
const initParamsComputed = computed(() => {
    return initParams;
});

const initParamsRef = ref(initParams);

const query = useQuery("get", "/objects/{objid}", initParamsComputed);

const files = computed(() => {
    return [
        {
            name: "foo",
            size: "500",
            lastModified: "ysterday",
        },
    ];
});

// get info about this snapshot, including the root object
</script>
