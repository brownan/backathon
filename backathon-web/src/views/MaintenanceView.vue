<template>
    <h1 class="title is-2">Repository Maintenance</h1>
    <div class="columns">
        <div class="column">
            <div class="panel">
                <div class="panel-heading">Garbage Collection</div>
                <div class="panel-block">
                    This will recover space in the remote repository by deleting data no
                    longer needed by any snapshot.
                </div>

                <div class="panel-block">
                    <div class="message is-info">
                        <div class="message-body">
                            <button
                                v-if="!garbageInfo"
                                class="button is-info"
                                type="button"
                                :disabled="garbageInfoFetching"
                                :class="{ 'is-loading': garbageInfoFetching }"
                                @click="fetchGarbageInfo"
                            >
                                Estimate Savings
                            </button>
                            <div
                                v-else
                                class="content"
                            >
                                <i>
                                    Note: unreachable object count and size are estimates
                                </i>
                                <table class="table is-narrow">
                                    <tbody>
                                        <tr>
                                            <th>Total Repository Objects</th>
                                            <td>
                                                {{
                                                    formatNumber(
                                                        garbageInfo.repoInfo.numObjects,
                                                    )
                                                }}
                                            </td>
                                        </tr>
                                        <tr>
                                            <th>Unreachable objects</th>
                                            <td>
                                                {{
                                                    formatNumber(
                                                        garbageInfo.unreachableCount,
                                                    )
                                                }}
                                                ({{
                                                    formatPercent(
                                                        garbageInfo.unreachableCount /
                                                            garbageInfo.repoInfo
                                                                .numObjects,
                                                    )
                                                }})
                                            </td>
                                        </tr>
                                        <tr>
                                            <th>Total Repository Size</th>
                                            <td>
                                                {{
                                                    formatFilesize(
                                                        garbageInfo.repoInfo.uploadedSize,
                                                    )
                                                }}
                                            </td>
                                        </tr>
                                        <tr>
                                            <th>Unreachable size</th>
                                            <td>
                                                {{
                                                    formatFilesize(
                                                        garbageInfo.unreachableSize,
                                                    )
                                                }}
                                                ({{
                                                    formatPercent(
                                                        garbageInfo.unreachableSize /
                                                            garbageInfo.repoInfo
                                                                .uploadedSize,
                                                    )
                                                }})
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>

<style scoped></style>

<script setup lang="ts">
import { client } from "@/api.ts";
import type { components } from "@/schema";
import { ref } from "vue";
import { formatNumber, formatFilesize, formatPercent } from "@/utils/formatting";

const garbageInfo = ref<components["schemas"]["GarbageCollectionInfo"] | null>(null);
const garbageInfoFetching = ref<boolean>(false);

function fetchGarbageInfo() {
    garbageInfoFetching.value = true;
    client
        .GET("/garbage")
        .then((response) => {
            garbageInfo.value = response.data || null;
        })
        .finally(() => {
            garbageInfoFetching.value = false;
        });
}
</script>
