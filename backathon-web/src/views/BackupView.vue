<template>
    <h1 class="title is-2">Backup</h1>

    <div class="columns">
        <div class="column">
            <div class="panel scan-box">
                <div class="panel-heading">Scan</div>
                <div class="panel-block">
                    <button
                        type="button"
                        class="button is-flex-grow-1 is-primary"
                        @click="doScan()"
                    >
                        Scan Now
                    </button>
                </div>
                <div class="panel-block">
                    <template v-if="scanInfo.data && needsScan">
                        <div class="content">
                            The following new paths need scanning
                            <ul>
                                <li
                                    v-for="item in scanInfo.data.unscanned"
                                    :key="item.id"
                                >
                                    {{ item.path }}
                                </li>
                            </ul>
                        </div>
                    </template>
                    <template v-else>
                        Run a scan to check for new and changed files within the backup
                        set
                    </template>
                </div>
                <div class="panel-block is-flex-direction-column">
                    <h3 class="subtitle is-3">Scan Results</h3>
                    <table
                        v-if="scanInfo.data"
                        class="table is-narrow"
                    >
                        <tbody>
                            <tr>
                                <th>Outdated Paths</th>
                                <td>
                                    <SpinnerIfScanning>
                                        {{
                                            formatter.format(scanInfo.data.outdatedCount)
                                        }}
                                    </SpinnerIfScanning>
                                </td>
                            </tr>
                            <tr>
                                <th>Outdated Size</th>
                                <td>
                                    <SpinnerIfScanning>
                                        {{ filesize(scanInfo.data.outdatedSize) }}
                                    </SpinnerIfScanning>
                                </td>
                            </tr>
                            <tr>
                                <th>Total Paths</th>
                                <td>
                                    <SpinnerIfScanning>
                                        {{ formatter.format(scanInfo.data.totalCount) }}
                                    </SpinnerIfScanning>
                                </td>
                            </tr>
                            <tr>
                                <th>Total Size</th>
                                <td>
                                    <SpinnerIfScanning>
                                        {{ filesize(scanInfo.data.totalSize) }}
                                    </SpinnerIfScanning>
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>

        <div class="column">
            <div class="panel backup-box">
                <div class="panel-heading">Backup</div>
                <div class="panel-block">
                    <button
                        type="button"
                        class="button is-flex-grow-1 is-primary"
                        @click="doBackup()"
                    >
                        Backup Now
                    </button>
                </div>
                <div class="panel-block is-flex-direction-column"></div>
            </div>
        </div>
    </div>
</template>

<style></style>

<script setup lang="ts">
import { client, useQuery } from "@/api";
import { computed, h, toRefs, type SetupContext } from "vue";
import { formatter } from "@/utils/formatting.ts";
import { filesize } from "filesize";
import { useJobStatus } from "@/utils/events.ts";
import SpinnerIcon from "@/components/SpinnerIcon.vue";

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

function SpinnerIfScanning(props, context: SetupContext) {
    if (scanStatus.value) {
        return h(SpinnerIcon);
    } else {
        return context.slots.default ? context.slots.default() : null;
    }
}

const { scan: scanStatus } = toRefs(useJobStatus());
</script>
