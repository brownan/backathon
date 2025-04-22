<template>
    <h1 class="title is-2">Backup</h1>

    <div
        class="message"
        :class="{ 'is-warning': needsScan }"
    >
        <div class="message-header">Scan</div>
        <div class="message-body content">
            <div>
                <button
                    type="button"
                    class="button"
                    @click="doScan()"
                >
                    Scan Now
                </button>
            </div>
            <template v-if="needsScan">
                The following items have been added and not yet scanned
                <ul v-if="itemsToScan.isReady">
                    <li
                        v-for="item in itemsToScan.data"
                        :key="item.id"
                    >
                        {{ item.path }}
                    </li>
                </ul>
            </template>
        </div>
    </div>

    <div class="card">
        <header class="card-header">
            <p class="card-header-title title is-4">Debug Actions</p>
        </header>
        <div class="card-content is-flex">
            <button
                type="button"
                class="button"
                @click="doBackup()"
            >
                Start Backup
            </button>
        </div>
    </div>
</template>

<style scoped></style>

<script setup lang="ts">
import { client, useQuery } from "@/api";
import { computed } from "vue";

const itemsToScan = useQuery({
    method: "get",
    url: "/scan/needed",
    options: {},
});

const needsScan = computed(() =>
    itemsToScan.isReady ? itemsToScan.data.length > 0 : false,
);

function doScan() {
    client.POST("/scan");
}

function doBackup() {
    client.POST("/backup");
}
</script>
