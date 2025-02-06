<template>
    <h1 class="title is-1">Restore Files</h1>
    <div class="columns">
        <div class="column">
            <h2 class="subtitle is-2">Snapshots</h2>
            <table class="table is-striped">
                <tbody>
                    <tr>
                        <th>ID</th>
                        <th>Path</th>
                        <th>Timestamp</th>
                        <th>Root</th>
                    </tr>
                    <tr
                        v-for="snapshot in snapshots"
                        :key="snapshot.id"
                    >
                        <td>{{ snapshot.id }}</td>
                        <td>
                            {{ snapshot.path }}
                        </td>
                        <td>
                            {{ new Date(snapshot.timestamp).toString() }}
                        </td>
                        <td>{{ snapshot.root }}</td>
                    </tr>
                </tbody>
            </table>
        </div>
        <div class="column">
            <h2 class="subtitle is-2">Files</h2>
        </div>
    </div>
</template>

<style scoped></style>

<script setup lang="ts">
import { useQuery } from "@/api.ts";

const { state: snapshots } = useQuery("get", "/snapshots");
</script>
