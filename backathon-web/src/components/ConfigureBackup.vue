<template>
    <div class="panel is-primary">
        <p class="panel-heading">Backup Roots</p>
        <div
            v-for="root in roots"
            :key="root.id"
            class="panel-block"
        >
            <button
                type="button"
                @click="confirm('really delete ' + root.path, () => deleted(root.path))"
            >
                <MdiIcon
                    class="mr-2"
                    :path="mdiDelete"
                />
            </button>
            {{ root.path }}
        </div>
    </div>
</template>

<style scoped></style>

<script setup lang="ts">
import { useQuery } from "@/api.ts";
import MdiIcon from "@/utils/MdiIcon.vue";
import { computed } from "vue";

import { mdiDelete } from "@mdi/js";
import { confirm } from "@/utils/confirm.ts";

const rootQuery = useQuery({
    method: "get",
    url: "/roots/",
    options: {},
});

const roots = computed(() => rootQuery.data || []);

function deleted(root) {
    console.log("Deleted", root);
}
</script>
