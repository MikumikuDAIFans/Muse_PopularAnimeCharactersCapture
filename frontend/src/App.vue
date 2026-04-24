<template>
  <div class="app">
    <nav class="sidebar">
      <div class="logo">Muse DataLoad</div>
      <router-link to="/">仪表盘</router-link>
      <router-link to="/tasks">任务管理</router-link>
      <router-link to="/data">数据浏览</router-link>
    </nav>
    <main class="content">
      <router-view />
    </main>
  </div>
</template>

<script setup>
</script>

<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: system-ui, sans-serif; background: #f5f5f5; color: #333; }
.app { display: flex; min-height: 100vh; }
.sidebar {
  width: 200px; background: #1a1a2e; color: #fff; padding: 20px;
}
.sidebar .logo { font-size: 18px; font-weight: bold; margin-bottom: 24px; }
.sidebar a {
  display: block; color: #ccc; text-decoration: none; padding: 10px 12px; border-radius: 6px;
}
.sidebar a:hover, .sidebar a.router-link-active { background: #16213e; color: #fff; }
.content { flex: 1; padding: 24px; }
</style>