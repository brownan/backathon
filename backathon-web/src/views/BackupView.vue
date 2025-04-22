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
            <template v-if="scanInfo.data && needsScan">
                The following new paths need scanning
                <ul>
                    <li
                        v-for="item in scanInfo.data.unscanned"
                        :key="item.id"
                    >
                        {{ item.path }}
                    </li>
                </ul>
            </template>
        </div>
    </div>

    <div class="message">
        <div class="message-header">Backup</div>
        <div class="message-body content">
            <h3 class="subtitle is-3">Ready to backup:</h3>
            <table
                v-if="scanInfo.data"
                class="table is-narrow"
            >
                <tbody>
                    <tr>
                        <th>Outdated Paths</th>
                        <td>{{ formatter.format(scanInfo.data.outdatedCount) }}</td>
                    </tr>
                    <tr>
                        <th>Outdated Size</th>
                        <td>{{ filesize(scanInfo.data.outdatedSize) }}</td>
                    </tr>
                    <tr>
                        <th>Total Paths</th>
                        <td>{{ formatter.format(scanInfo.data.totalCount) }}</td>
                    </tr>
                    <tr>
                        <th>Total Size</th>
                        <td>{{ filesize(scanInfo.data.totalSize) }}</td>
                    </tr>
                </tbody>
            </table>
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
import { formatter } from "@/utils/formatting.ts";
import { filesize } from "filesize";

const scanInfo = useQuery({
    method: "get",
    url: "/scan/info",
    options: {},
});

const needsScan = computed(() =>
    scanInfo.data ? scanInfo.data.unscanned.length > 0 : false,
);

function doScan() {
    client.POST("/scan");
}

function doBackup() {
    client.POST("/backup");
}
</script>
