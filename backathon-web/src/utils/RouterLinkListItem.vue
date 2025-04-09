<template>
    <RouterLink
        v-bind="$props"
        custom
        v-slot="{ isActive, href, navigate }"
    >
        <li :class="{ 'is-active': isActive }">
            <a
                :href="href"
                @click="navigate"
            >
                <slot></slot>
            </a>
        </li>
    </RouterLink>
</template>

<script setup lang="ts">
import { RouterLink, type RouterLinkProps } from "vue-router";

defineProps<RouterLinkProps>();
</script>
