<template>
    <div class="panel is-primary">
        <p class="panel-heading">Backup Roots</p>
        <div class="panel-block">
            <div class="columns is-flex-grow-1">
                <div class="column is-two-thirds">
                    <FileBrowser />
                </div>
                <div class="column content">
                    Current Roots:
                    <ul v-if="rootQuery.data">
                        <li
                            v-for="root in rootQuery.data"
                            :key="root.key"
                        >
                            {{ root.path }}
                        </li>
                    </ul>
                </div>
            </div>
        </div>
    </div>
</template>

<style></style>

<script setup lang="ts">
import { useQuery } from "@/api.ts";

import FileBrowser from "@/components/FileBrowser.vue";

const rootQuery = useQuery({
    method: "get",
    url: "/roots/",
    options: {},
});
</script>
