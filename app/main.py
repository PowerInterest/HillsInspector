from nicegui import ui
import app.ui as ui_module

# Initialize the UI
ui_module.init_ui()

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(title='HillsInspector', storage_secret='secret', port=8089, reload=False)
