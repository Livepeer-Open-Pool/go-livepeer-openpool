// Applies the visitor's choice of theme before the page is painted, so a
// page never flashes in the other one. With no choice made, the page
// follows the system. Loaded by every Open Pool site ahead of its styles.
(() => {
  const key = "openpool-theme"
  const root = document.documentElement
  const read = () => {
    try {
      const stored = localStorage.getItem(key)
      return stored === "light" || stored === "dark" ? stored : "system"
    } catch {
      // Storage is closed to this page: follow the system.
      return "system"
    }
  }
  const apply = (choice) => {
    if (choice === "light" || choice === "dark") root.setAttribute("data-theme", choice)
    else root.removeAttribute("data-theme")
  }
  let current = read()
  apply(current)
  window.openPoolTheme = {
    get: () => current,
    set: (choice) => {
      current = choice === "light" || choice === "dark" ? choice : "system"
      apply(current)
      try {
        if (current === "system") localStorage.removeItem(key)
        else localStorage.setItem(key, current)
      } catch {
        // The choice holds for this page; it cannot be remembered.
      }
    },
  }
})()
