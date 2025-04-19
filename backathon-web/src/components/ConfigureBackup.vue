<template>
    <div class="panel is-primary">
        <p class="panel-heading">Backup Roots</p>
        <div class="panel-block">
            <FileBrowser />
        </div>
    </div>
</template>

<style></style>

<script setup lang="ts">
import { useQuery } from "@/api.ts";
import { ref, watch } from "vue";

import FileBrowser from "@/components/FileBrowser.vue";

const rootQuery = useQuery({
    method: "get",
    url: "/roots/",
    options: {},
});

const roots = ref<string[]>([]);

watch(
    () => rootQuery.data,
    (newData) => {
        if (newData) {
            console.debug("Updating new root data:", newData);
            roots.value = newData.map((p) => p.key);
        }
    },
);
</script>
