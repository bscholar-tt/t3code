import { DesktopNotificationPayloadSchema } from "@t3tools/contracts";
import * as Electron from "electron";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import * as IpcChannels from "../channels.ts";
import { makeIpcMethod } from "../DesktopIpc.ts";

export const showNotification = makeIpcMethod({
  channel: IpcChannels.SHOW_NOTIFICATION_CHANNEL,
  payload: DesktopNotificationPayloadSchema,
  result: Schema.Void,
  handler: Effect.fn("desktop.ipc.notification.show")(function* (payload) {
    // Skip if any window is currently focused — user is already looking at the app
    const focused = Electron.BrowserWindow.getFocusedWindow();
    if (focused && !focused.isDestroyed()) {
      return;
    }

    // macOS uses the app bundle's .icns as the notification icon automatically;
    // setting `icon` here would add a secondary content image on the right instead.
    const notification = new Electron.Notification({
      title: payload.title,
      body: payload.body,
    });

    const navigatePayload = {
      environmentId: payload.threadEnvironmentId,
      threadId: payload.threadId,
    };

    // The click handler fires asynchronously after IPC resolution so raw
    // Electron APIs are used here instead of the Effect-wrapped services.
    notification.on("click", () => {
      const win = Electron.BrowserWindow.getAllWindows()[0];
      if (!win || win.isDestroyed()) return;
      if (win.isMinimized()) win.restore();
      if (!win.isVisible()) win.show();
      Electron.app.focus({ steal: true });
      win.focus();
      win.webContents.send(IpcChannels.NOTIFICATION_NAVIGATE_CHANNEL, navigatePayload);
    });

    notification.show();
  }),
});
