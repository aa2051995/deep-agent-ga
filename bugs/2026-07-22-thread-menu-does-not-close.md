# Thread action menu (Rename/Delete) does not disappear

**Date:** 2026-07-22
**Area:** UI — sidebar thread list

## Symptom

Opening a thread's `⋯` action menu (Rename / Delete) and then clicking elsewhere
— including the **New research** button — left the menu open. It would not
dismiss on the next click.

## Root cause

The menu was dismissed by a single `window`-level `"click"` listener
(`setOpenThreadMenu(null)`), while the menu container called
`event.stopPropagation()` on its `onClick`. React attaches its delegated
listeners at the root container, and a synthetic `stopPropagation()` calls the
native `stopPropagation()` at that boundary — so the intended "close on outside
click" listener on `window` was inconsistently prevented from firing. The result
was an outside-click-to-close handler that could be silently swallowed, leaving
the menu stuck open.

## Related files

- `ui/src/App.tsx` — thread list render + the menu open/close effect (`E1`) and
  the `.thread-menu-wrap` / `.thread-menu-trigger` markup.

## Solution

Replace the fragile pattern with a scoped outside-`mousedown` handler that only
runs while a menu is open and checks DOM containment against a ref on the open
menu wrapper:

- Add `threadMenuRef` and attach it to the wrapper of the currently-open row
  (`ref={openThreadMenu === thread.threadId ? threadMenuRef : undefined}`).
- The effect is keyed on `openThreadMenu`; it registers `document`
  `mousedown` + `keydown(Escape)` only while open, and closes when the click is
  outside `threadMenuRef.current`.
- The trigger keeps `stopPropagation()` + a toggle so re-clicking it still
  closes; menu items close via their own handlers.

## Best practices

- For "click outside to close" popovers, detect via a `mousedown`/`pointerdown`
  listener + a `ref.contains(target)` check, not a bubbling `click` listener
  combined with `stopPropagation()` inside the popover.
- Register the outside listener only while the popover is open (effect keyed on
  the open state) so it can't fire in unexpected states.
- Prefer `mousedown` over `click` so the menu closes before focus/selection side
  effects of the new click land.
