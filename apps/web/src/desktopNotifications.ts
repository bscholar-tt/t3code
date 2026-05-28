import type { ClientSettings, EnvironmentId, ThreadId } from "@t3tools/contracts";

import { getClientSettings } from "~/hooks/useSettings";
import type { SidebarThreadSummary } from "./types";

const lastNotificationByThread = new Map<string, number>();
const NOTIFICATION_COOLDOWN_MS = 5_000;

export function maybeFireDesktopNotification(
  environmentId: EnvironmentId,
  threadId: ThreadId,
  previous: SidebarThreadSummary | undefined,
  next: SidebarThreadSummary,
): void {
  const bridge = typeof window !== "undefined" ? window.desktopBridge : undefined;
  if (!bridge?.showDesktopNotification) return;

  const settings = getClientSettings();
  if (
    !settings.desktopNotifyOnTurnCompleted &&
    !settings.desktopNotifyOnInputNeeded &&
    !settings.desktopNotifyOnError
  ) {
    return;
  }

  const key = `${environmentId}:${threadId}`;
  const now = Date.now();
  const last = lastNotificationByThread.get(key);
  if (last !== undefined && now - last < NOTIFICATION_COOLDOWN_MS) return;

  const notification = deriveNotification(previous, next, settings);
  if (!notification) return;

  lastNotificationByThread.set(key, now);

  void bridge.showDesktopNotification({
    title: notification.title,
    body: notification.body,
    threadEnvironmentId: environmentId,
    threadId,
  });
}

function deriveNotification(
  previous: SidebarThreadSummary | undefined,
  next: SidebarThreadSummary,
  settings: ClientSettings,
): { title: string; body: string } | null {
  const threadLabel = next.title || "Thread";

  if (
    settings.desktopNotifyOnError &&
    next.session?.status === "error" &&
    previous?.session?.status !== "error"
  ) {
    return { title: "Error", body: threadLabel };
  }

  if (
    settings.desktopNotifyOnInputNeeded &&
    next.hasPendingApprovals &&
    !previous?.hasPendingApprovals
  ) {
    return { title: "Approval needed", body: threadLabel };
  }

  if (
    settings.desktopNotifyOnInputNeeded &&
    next.hasPendingUserInput &&
    !previous?.hasPendingUserInput
  ) {
    return { title: "Input needed", body: threadLabel };
  }

  if (settings.desktopNotifyOnTurnCompleted) {
    const prevTurnState = previous?.latestTurn?.state;
    const nextTurnState = next.latestTurn?.state;
    if (
      nextTurnState === "completed" &&
      prevTurnState !== "completed" &&
      prevTurnState !== undefined
    ) {
      return { title: "Turn completed", body: threadLabel };
    }
  }

  return null;
}
