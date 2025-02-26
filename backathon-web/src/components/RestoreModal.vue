<template>
    <StyledModal title="Restore">
        <table class="table">
            <tbody>
                <tr>
                    <th>Name</th>
                    <td>{{ name }}</td>
                </tr>
                <tr>
                    <th>Type</th>
                    <td v-if="obj?.type === 'file'">File</td>
                    <td v-else-if="obj?.type === 'tree'">Directory</td>
                    <td v-else-if="obj?.type === 'symlink'">Symbolic Link</td>
                    <td v-else>Other</td>
                </tr>
                <tr>
                    <th>Size</th>
                    <td>
                        {{ obj?.type === "file" ? obj?.file_size : "-" }}
                    </td>
                </tr>
                <tr>
                    <th>Last Modified</th>
                    <td>
                        {{
                            obj?.last_modified_time
                                ? new Date(obj.last_modified_time).toLocaleString()
                                : ""
                        }}
                    </td>
                </tr>
            </tbody>
        </table>
        <template v-slot:buttons>
            <a
                :href="downloadUrl"
                class="button is-link"
            >
                Download
            </a>
        </template>
    </StyledModal>
</template>

<style scoped></style>

<script setup lang="ts">
import StyledModal from "@/components/StyledModal.vue";
import { type components } from "@/schema";
import { computed } from "vue";

type RestoreProps = {
    obj: components["schemas"]["Object"];
    name: string;
};
const props = defineProps<RestoreProps>();

const downloadUrl = computed(() => {
    const url = new URL(`/api/objects/${props.obj.objid}/download`, window.location);
    url.searchParams.append("name", props.name);
    return url.toString();
});
</script>
