<template>
    <h1 class="title is-2">Backup</h1>
    <div class="card">
        <header class="card-header">
            <p class="card-header-title title is-4">Debug Actions</p>
        </header>
        <div class="card-content is-flex">
            <button
                type="button"
                class="button"
                @click="doScan()"
            >
                Start Scan
            </button>
            <button
                type="button"
                class="button"
            >
                Start Backup
            </button>
        </div>
    </div>
</template>

<style scoped></style>

<script setup lang="ts">
import { client } from "@/api";

function doScan() {
    client.POST("/scan");
}
</script>
