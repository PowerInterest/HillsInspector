from nicegui import ui
import pandas as pd
from datetime import datetime
import asyncio
import sys
from pathlib import Path

# Ensure we can import from src
sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.database import get_connection
from src.db.operations import PropertyDB

def init_ui():
    @ui.page('/')
    def main_page():
        # Theme colors
        ui.colors(primary='#0f172a', secondary='#334155', accent='#0ea5e9')
        
        # State
        state = {'selected_case': None}
        
        # Header
        with ui.header().classes('bg-primary text-white items-center p-4'):
            ui.icon('real_estate_agent', size='32px').classes('mr-2')
            ui.label('Hillsborough Property Inspector').classes('text-2xl font-bold')
            ui.space()
            with ui.row().classes('items-center'):
                ui.label('Status: Connected').classes('text-sm text-green-400')

        # Main Layout
        with ui.row().classes('w-full h-full'):
            
            # Sidebar / Navigation
            with ui.column().classes('w-64 bg-slate-100 h-screen p-4 border-r'):
                ui.label('Menu').classes('text-gray-500 font-bold mb-2')
                tabs = ui.tabs().classes('w-full flex-col text-left')
                with tabs:
                    ui.tab('Dashboard', icon='dashboard').classes('justify-start w-full')
                    ui.tab('Auctions', icon='gavel').classes('justify-start w-full')
                    ui.tab('Analysis', icon='analytics').classes('justify-start w-full')
                    ui.tab('Settings', icon='settings').classes('justify-start w-full')
            
            # Content Area
            with ui.column().classes('flex-1 p-6 h-screen overflow-y-auto'):
                with ui.tab_panels(tabs, value='Dashboard').classes('w-full bg-transparent'):
                    
                    # --- DASHBOARD TAB ---
                    with ui.tab_panel('Dashboard'):
                        ui.label('Overview').classes('text-3xl font-bold mb-6')
                        
                        # Stats Cards
                        with ui.row().classes('w-full gap-4 mb-8'):
                            with ui.card().classes('w-64 p-4 bg-blue-50 cursor-pointer hover:shadow-lg transition-shadow').on('click', lambda: tabs.set_value('Auctions')):
                                ui.label('Total Auctions').classes('text-gray-500')
                                ui.label('Loading...').bind_text_from(stats, 'total')
                                
                            with ui.card().classes('w-64 p-4 bg-green-50 cursor-pointer hover:shadow-lg transition-shadow').on('click', lambda: tabs.set_value('Auctions')):
                                ui.label('Analyzed').classes('text-gray-500')
                                ui.label('Loading...').bind_text_from(stats, 'analyzed')
                                
                            with ui.card().classes('w-64 p-4 bg-purple-50 cursor-pointer hover:shadow-lg transition-shadow').on('click', lambda: tabs.set_value('Auctions')):
                                ui.label('Opportunities').classes('text-gray-500')
                                ui.label('Loading...').bind_text_from(stats, 'opportunities')

                    # --- AUCTIONS TAB ---
                    with ui.tab_panel('Auctions'):
                        ui.label('Auction Calendar').classes('text-3xl font-bold mb-6')
                        
                        # Auction Table
                        ui.label('Click on any row to view full analysis details.').classes('text-gray-500 mb-4 italic')
                        columns = [
                            {'name': 'date', 'label': 'Date', 'field': 'auction_date', 'sortable': True},
                            {'name': 'case', 'label': 'Case #', 'field': 'case_number', 'sortable': True},
                            {'name': 'address', 'label': 'Address', 'field': 'property_address'},
                            {'name': 'judgment', 'label': 'Judgment', 'field': 'formatted_judgment', 'sortable': True},
                            {'name': 'status', 'label': 'Status', 'field': 'status', 'sortable': True},
                        ]
                        
                        def load_auctions():
                            conn = get_connection()
                            df = conn.execute("SELECT * FROM auctions ORDER BY auction_date DESC").fetchdf()
                            conn.close()
                            
                            # Convert all timestamp columns
                            for col in df.columns:
                                if pd.api.types.is_datetime64_any_dtype(df[col]):
                                    df[col] = df[col].astype(str)
                                
                            records = df.to_dict('records')
                            for r in records:
                                val = r.get('final_judgment_amount')
                                r['formatted_judgment'] = f"${val:,.2f}" if val else "-"
                            return records

                        rows = load_auctions()
                        
                        table = ui.table(columns=columns, rows=rows, row_key='case_number').classes('w-full')
                        table.add_slot('body-cell-case', '''
                            <q-td :props="props">
                                <a href="#" @click.prevent="$parent.$emit('open_case', props.value)" class="text-blue-600 hover:underline">{{ props.value }}</a>
                            </q-td>
                        ''')
                        
                        # Handle row click (custom event logic would be needed for pure NiceGUI, 
                        # but for simplicity we'll use a selection mode or button)
                        
                        def open_selected_case(e):
                            # e.args is the row data
                            row = e.args[1]
                            state['selected_case'] = row['case_number']
                            tabs.set_value('Analysis')
                            refresh_analysis(row['case_number'])

                        table.on('rowClick', open_selected_case)


                    # --- ANALYSIS TAB ---
                    with ui.tab_panel('Analysis'):
                        with ui.row().classes('items-center justify-between w-full mb-6'):
                            ui.label('Property Analysis').classes('text-3xl font-bold')
                            ui.button('Refresh Data', icon='refresh', on_click=lambda: refresh_analysis(state['selected_case']))
                        
                        # Content container
                        analysis_container = ui.column().classes('w-full gap-6')
                        
                        def refresh_analysis(case_num):
                            analysis_container.clear()
                            if not case_num:
                                with analysis_container:
                                    ui.label('Select an auction from the Auctions tab to view details.').classes('text-gray-500 italic')
                                return

                            conn = get_connection()
                            
                            # Fetch Auction & Parcel Data
                            auction = conn.execute("SELECT * FROM auctions WHERE case_number = ?", [case_num]).fetchone()
                            if not auction:
                                return
                                
                            # Map columns
                            cols = [d[0] for d in conn.description]
                            auc_data = dict(zip(cols, auction))
                            
                            # Convert dates in auc_data
                            for k, v in auc_data.items():
                                if isinstance(v, (datetime, pd.Timestamp)):
                                    auc_data[k] = str(v)
                                elif hasattr(v, 'isoformat'): # date objects
                                    auc_data[k] = v.isoformat()
                            
                            # Fetch Parcel
                            parcel = conn.execute("SELECT * FROM parcels WHERE folio = ?", [auc_data.get('folio')]).fetchone()
                            par_data = {}
                            if parcel:
                                p_cols = [d[0] for d in conn.description]
                                par_data = dict(zip(p_cols, parcel))
                                # Convert dates in par_data
                                for k, v in par_data.items():
                                    if isinstance(v, (datetime, pd.Timestamp)):
                                        par_data[k] = str(v)
                                    elif hasattr(v, 'isoformat'):
                                        par_data[k] = v.isoformat()
                            
                            # Fetch Liens
                            liens = conn.execute("SELECT * FROM liens WHERE case_number = ?", [case_num]).fetchall()
                            l_cols = [d[0] for d in conn.description]
                            lien_rows = [dict(zip(l_cols, row)) for row in liens]
                            
                            # Convert dates in lien_rows
                            for row in lien_rows:
                                for k, v in row.items():
                                    if isinstance(v, (datetime, pd.Timestamp)):
                                        row[k] = str(v)
                                    elif hasattr(v, 'isoformat'):
                                        row[k] = v.isoformat()
                            
                            conn.close()
                            
                            with analysis_container:
                                # Top Info Card
                                with ui.card().classes('w-full p-6 bg-white shadow-sm'):
                                    with ui.row().classes('w-full justify-between items-start'):
                                        with ui.column().classes('flex-1'):
                                            ui.label(auc_data.get('property_address')).classes('text-2xl font-bold')
                                            ui.label(f"Case: {case_num}").classes('text-gray-500')
                                            ui.label(f"Owner: {par_data.get('owner_name', 'Unknown')}")
                                            
                                            # Status Chip
                                            status = auc_data.get('status', 'PENDING')
                                            color = 'green' if status == 'ANALYZED' else 'orange'
                                            ui.chip(status, color=color).classes('text-white font-bold mt-2')
                                            ui.label(f"Auction: {auc_data.get('auction_date')}").classes('font-bold mt-1')

                                        # Image Section
                                        with ui.column().classes('w-1/3'):
                                            img_url = par_data.get('image_url')
                                            if img_url:
                                                ui.image(img_url).classes('rounded-lg shadow-md w-full h-48 object-cover')
                                            else:
                                                # Fallback to Google Maps Embed
                                                addr = auc_data.get('property_address', '').replace(' ', '+')
                                                ui.html(f'''
                                                    <iframe 
                                                        width="100%" 
                                                        height="200" 
                                                        frameborder="0" 
                                                        style="border:0; border-radius: 8px;" 
                                                        src="https://maps.google.com/maps?q={addr}&t=&z=13&ie=UTF8&iwloc=&output=embed" 
                                                        allowfullscreen>
                                                    </iframe>
                                                ''').classes('w-full h-48 rounded-lg shadow-md')

                                # Financials Row
                                with ui.row().classes('w-full gap-4'):
                                    # Value Card
                                    with ui.card().classes('flex-1 p-4'):
                                        ui.label('Financials').classes('text-lg font-bold mb-4 text-primary')
                                        with ui.grid(columns=2).classes('w-full gap-2'):
                                            ui.label('Assessed Value:')
                                            ui.label(f"${par_data.get('assessed_value', 0):,.2f}").classes('font-mono')
                                            
                                            ui.label('Final Judgment:')
                                            ui.label(f"${auc_data.get('final_judgment_amount', 0):,.2f}").classes('font-mono text-red-600')
                                            
                                            ui.label('Opening Bid:')
                                            ui.label(f"${auc_data.get('opening_bid', 0):,.2f}").classes('font-mono')

                                    # Property Specs
                                    with ui.card().classes('flex-1 p-4'):
                                        ui.label('Property Specs').classes('text-lg font-bold mb-4 text-primary')
                                        with ui.grid(columns=2).classes('w-full gap-2'):
                                            ui.label('Year Built:')
                                            ui.label(str(par_data.get('year_built', '-')))
                                            
                                            ui.label('Living Area:')
                                            ui.label(f"{par_data.get('heated_area', 0):,} sqft")
                                            
                                            ui.label('Beds/Baths:')
                                            ui.label(f"{par_data.get('beds', '-')}/{par_data.get('baths', '-')}")

                                # Liens Section
                                with ui.card().classes('w-full p-4'):
                                    ui.label('Lien Research').classes('text-lg font-bold mb-4 text-primary')
                                    
                                    if lien_rows:
                                        l_columns = [
                                            {'name': 'type', 'label': 'Type', 'field': 'document_type'},
                                            {'name': 'date', 'label': 'Recorded', 'field': 'recording_date'},
                                            {'name': 'amount', 'label': 'Amount', 'field': 'formatted_amount'},
                                            {'name': 'survives', 'label': 'Survives?', 'field': 'formatted_survives'},
                                        ]
                                        
                                        # Format rows
                                        for l in lien_rows:
                                            amt = l.get('amount')
                                            l['formatted_amount'] = f"${amt:,.2f}" if amt else "-"
                                            surv = l.get('is_surviving')
                                            l['formatted_survives'] = "YES" if surv else "No"
                                            
                                        ui.table(columns=l_columns, rows=lien_rows).classes('w-full')
                                        
                                        # Calculate Surviving Total
                                        surviving_total = sum(l['amount'] for l in lien_rows if l.get('is_surviving') and l.get('amount'))
                                        ui.label(f"Total Surviving Liens: ${surviving_total:,.2f}").classes('text-xl font-bold text-red-600 mt-4 text-right w-full')
                                    else:
                                        ui.label('No liens found or analysis pending.').classes('italic text-gray-500')

                                # Document Analysis Section
                                with ui.card().classes('w-full p-4'):
                                    ui.label('Document Analysis').classes('text-lg font-bold mb-4 text-primary')
                                    
                                    with ui.row().classes('w-full gap-4'):
                                        # Final Judgment Text
                                        with ui.column().classes('flex-1'):
                                            ui.label('Final Judgment (OCR)').classes('font-bold text-gray-600')
                                            fj_text = auc_data.get('final_judgment_content')
                                            if fj_text:
                                                ui.textarea(value=fj_text).props('readonly autogrow').classes('w-full bg-gray-50')
                                            else:
                                                ui.label('No judgment text available.').classes('italic text-gray-400')
                                        
                                        # Market Analysis Text
                                        with ui.column().classes('flex-1'):
                                            ui.label('Market Data (OCR)').classes('font-bold text-gray-600')
                                            ma_text = par_data.get('market_analysis_content')
                                            if ma_text:
                                                ui.textarea(value=ma_text).props('readonly autogrow').classes('w-full bg-gray-50')
                                            else:
                                                ui.label('No market data text available.').classes('italic text-gray-400')

                                # Actions
                                with ui.row().classes('w-full gap-4 mt-4'):
                                    async def run_analysis():
                                        if not case_num:
                                            return
                                        
                                        ui.notify(f'Starting analysis for {case_num}...', type='info')
                                        
                                        # Run the analysis script as a subprocess
                                        proc = await asyncio.create_subprocess_exec(
                                            sys.executable, 'analyze_property.py', case_num,
                                            stdout=asyncio.subprocess.PIPE,
                                            stderr=asyncio.subprocess.PIPE,
                                            cwd=str(Path(__file__).resolve().parents[1])
                                        )
                                        
                                        # We could stream output, but for now just wait
                                        stdout, stderr = await proc.communicate()
                                        
                                        if proc.returncode == 0:
                                            ui.notify('Analysis complete!', type='positive')
                                            refresh_analysis(case_num)
                                            stats.refresh()
                                        else:
                                            ui.notify(f'Analysis failed: {stderr.decode()}', type='negative')
                                            print(f"Analysis Error: {stderr.decode()}")

                                    ui.button('Run Full Analysis', icon='play_arrow', color='accent', on_click=run_analysis).classes('w-full')

                        # Initial load
                        refresh_analysis(state['selected_case'])

                    # --- SETTINGS / INGESTION TAB ---
                    with ui.tab_panel('Settings'):
                        ui.label('System Management').classes('text-3xl font-bold mb-6')
                        
                        with ui.card().classes('w-full p-6'):
                            ui.label('Data Ingestion').classes('text-xl font-bold mb-4')
                            ui.label('Scrape upcoming auctions from Foreclosure and Tax Deed sites.').classes('text-gray-500 mb-4')
                            
                            async def run_ingestion():
                                ui.notify('Starting ingestion... This may take a while.', type='info')
                                proc = await asyncio.create_subprocess_exec(
                                    sys.executable, 'src/services/ingestion.py',
                                    stdout=asyncio.subprocess.PIPE,
                                    stderr=asyncio.subprocess.PIPE,
                                    cwd=str(Path(__file__).resolve().parents[1])
                                )
                                stdout, stderr = await proc.communicate()
                                if proc.returncode == 0:
                                    ui.notify('Ingestion complete!', type='positive')
                                    # We need to refresh stats if possible, but stats object might not be available here if defined later?
                                    # Assuming stats is available in scope or we can reload page
                                    ui.notify('Please refresh the page to see new data.', type='warning')
                                else:
                                    ui.notify(f'Ingestion failed: {stderr.decode()}', type='negative')

                            ui.button('Scrape Upcoming Auctions (60 Days)', icon='cloud_download', on_click=run_ingestion)

    # Stats State
    class Stats:
        def __init__(self):
            self.total = 0
            self.analyzed = 0
            self.opportunities = 0
            
        def refresh(self):
            conn = get_connection()
            self.total = conn.execute("SELECT COUNT(*) FROM auctions").fetchone()[0]
            self.analyzed = conn.execute("SELECT COUNT(*) FROM auctions WHERE status = 'ANALYZED'").fetchone()[0]
            # Simple logic for opportunities
            self.opportunities = conn.execute("SELECT COUNT(*) FROM auctions WHERE status = 'ANALYZED'").fetchone()[0] 
            conn.close()

    stats = Stats()
    stats.refresh()
