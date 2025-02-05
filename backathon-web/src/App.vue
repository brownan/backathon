<template>
    <nav class="navbar" role="navigation" aria-label="main navigation">
        <div class="navbar-brand">
            <a class="navbar-item">
                <h1 class="title">Backathon</h1>
            </a>
            <a
                role="button"
                class="navbar-burger"
                :class="navbarActive ? 'is-active' : ''"
                aria-label="menu"
                aria-expanded="false"
                data-target="navbar-content"
                v-on:click="navbarActive = !navbarActive"
            >
                <span aria-hidden="true"></span>
                <span aria-hidden="true"></span>
                <span aria-hidden="true"></span>
                <span aria-hidden="true"></span>
            </a>
        </div>

        <div id="navbar-content" class="navbar-menu" :class="navbarActive ? 'is-active' : ''">
            <div class="navbar-start">
                <a class="navbar-item"> Status </a>
                <a class="navbar-item"> Configure </a>
                <a class="navbar-item"> Backup </a>
                <a class="navbar-item"> Restore </a>
            </div>
        </div>
    </nav>
    <div class="section">
        <RouterView />
    </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { RouterView } from 'vue-router'

const navbarActive = ref(false)
</script>
