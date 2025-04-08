<template>
    <div
        id="status-footer"
        :class="{ 'status-open': statusOpen }"
    >
        <div
            class="scan-status"
            v-if="jobStatus.scan"
        >
            <div>
                Scanning
                <span class="scan-count">
                    {{ formatter.format(jobStatus.scan.scanned) }}
                    <template v-if="jobStatus.scan.total">
                        / {{ formatter.format(jobStatus.scan.total) }} ({{
                            `${Math.floor(
                                (jobStatus.scan.scanned / jobStatus.scan.total) * 100,
                            )}%`
                        }})
                    </template>
                </span>
            </div>
            <div class="last-path">{{ jobStatus.scan.last_path }}</div>
            <progress
                v-if="jobStatus.scan.total"
                class="progress is-info"
                :value="jobStatus.scan.scanned"
                :max="jobStatus.scan.total"
            >
                {{
                    `${Math.floor(
                        (jobStatus.scan.scanned / jobStatus.scan.total) * 100,
                    )}%`
                }}
            </progress>
        </div>

        <div
            class="backup-status"
            v-if="jobStatus.backup"
        >
            <div>
                Backing up
                {{ formatter.format(jobStatus.backup.count_progress) }}
                / {{ formatter.format(jobStatus.backup.count_total) }} items,
                {{ filesize(jobStatus.backup.size_progress) }}
                / {{ filesize(jobStatus.backup.size_total) }}
            </div>
        </div>
    </div>
</template>

<style>
#main-view:has(~ #status-footer.status-open) {
    margin-bottom: 6em;
}
#status-footer {
    overflow: hidden;
    position: fixed;
    bottom: 0;
    width: 100%;
    height: 0;
    background-color: var(--body-background-color);
    transition: height 0.5s linear;

    display: flex;
    justify-content: center;
    align-items: center;
}
#status-footer.status-open {
    height: 6em;
    border-top: 2px solid var(--border);
}

#status-footer .scan-status,
#status-footer .backup-status {
    display: flex;
    flex-direction: column;
    width: 100%;
    margin: 0 5em 0 5em;
}
</style>

<script setup lang="ts">
import { useJobStatus } from "@/utils/status.ts";
import { computed } from "vue";
import { filesize } from "filesize";

const jobStatus = useJobStatus();

const statusOpen = computed(() => jobStatus.scan || jobStatus.backup);

const formatter = Intl.NumberFormat();
</script>
