import os
from datetime import datetime, timedelta
from collections import defaultdict
from flask import Flask, redirect, url_for, request, render_template, session, flash, jsonify
from kommo_client import KommoClient
from token_storage import TokenStorage

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-change-me')

storage = TokenStorage()
kommo = KommoClient(token_storage=storage)

# Kommo system status IDs
STATUS_WON = 142
STATUS_LOST = 143


# ─── Auth routes ───

@app.route('/')
def index():
    if kommo.is_authenticated():
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))


@app.route('/auth/login')
def login():
    auth_url = (
        f"https://www.kommo.com/oauth"
        f"?client_id={kommo.client_id}"
        f"&state=kommo_auth"
        f"&mode=post_message"
    )
    return render_template('login.html', authenticated=False, auth_url=auth_url)


@app.route('/auth/callback')
def callback():
    code = request.args.get('code')
    if not code:
        flash('Codigo de autorizacao nao recebido.', 'danger')
        return redirect(url_for('login'))
    referer = request.args.get('referer', kommo.subdomain)
    subdomain = referer.split('.')[0] if '.' in referer else referer
    try:
        kommo.exchange_code(code, subdomain=subdomain)
        flash('Conectado com sucesso ao Kommo CRM!', 'success')
        return redirect(url_for('dashboard'))
    except Exception as e:
        return render_template('error.html', authenticated=False,
                               error_message=f"Erro ao obter token: {e}")


@app.route('/auth/logout')
def logout():
    storage.delete_token()
    session.clear()
    flash('Desconectado com sucesso.', 'info')
    return redirect(url_for('login'))


# ─── Data helpers ───

def build_pipelines_map():
    pipelines_data = kommo.get_pipelines()
    pipelines = {}
    status_map = {}
    if pipelines_data and '_embedded' in pipelines_data:
        for p in pipelines_data['_embedded'].get('pipelines', []):
            statuses = {}
            if '_embedded' in p:
                for s in p['_embedded'].get('statuses', []):
                    statuses[s['id']] = {
                        'name': s['name'],
                        'sort': s.get('sort', 0),
                        'type': s.get('type', 0),
                        'pipeline_id': p['id'],
                    }
                    status_map[s['id']] = s['name']
            pipelines[p['id']] = {
                'name': p['name'],
                'is_archive': p.get('is_archive', False),
                'statuses': statuses,
            }
    return pipelines, status_map


def build_users_map():
    users_data = kommo.get_users()
    users = {}
    if users_data and '_embedded' in users_data:
        for u in users_data['_embedded'].get('users', []):
            users[u['id']] = u['name']
    return users


def get_custom_field_values(lead, field_id):
    """Extract custom field value(s) from a lead."""
    for cf in lead.get('custom_fields_values') or []:
        if cf.get('field_id') == field_id:
            vals = cf.get('values', [])
            return [v.get('value', '') for v in vals]
    return []


def find_custom_field_id(fields_data, field_name_contains):
    """Find a custom field ID by partial name match."""
    if not fields_data or '_embedded' not in fields_data:
        return None
    for f in fields_data['_embedded'].get('custom_fields', []):
        if field_name_contains.lower() in f.get('name', '').lower():
            return f['id'], f.get('name', ''), f.get('enums')
    return None


# ─── Dashboard route ───

@app.route('/dashboard')
def dashboard():
    if not kommo.is_authenticated():
        return redirect(url_for('login'))

    try:
        account = kommo.get_account()
        account_name = account.get('name', 'Kommo CRM')

        pipelines, status_map = build_pipelines_map()
        users = build_users_map()

        # Discover custom fields for source, lead type, etc.
        custom_fields_data = kommo.get_custom_fields('leads')
        source_field = find_custom_field_id(custom_fields_data, 'fonte')
        lead_type_field = find_custom_field_id(custom_fields_data, 'tipo')
        interest_field = find_custom_field_id(custom_fields_data, 'momento')
        city_field = find_custom_field_id(custom_fields_data, 'cidade')
        sound_field = find_custom_field_id(custom_fields_data, 'sonori')
        decisor_field = find_custom_field_id(custom_fields_data, 'decisor')
        venue_field = find_custom_field_id(custom_fields_data, 'local')
        event_date_field = find_custom_field_id(custom_fields_data, 'data do evento')

        source_field_id = source_field[0] if source_field else None
        lead_type_field_id = lead_type_field[0] if lead_type_field else None
        interest_field_id = interest_field[0] if interest_field else None
        city_field_id = city_field[0] if city_field else None
        sound_field_id = sound_field[0] if sound_field else None
        decisor_field_id = decisor_field[0] if decisor_field else None
        venue_field_id = venue_field[0] if venue_field else None
        event_date_field_id = event_date_field[0] if event_date_field else None

        # ─── Parse filters ───
        f_pipeline = request.args.get('pipeline', '')
        f_statuses = request.args.getlist('statuses')
        f_period_type = request.args.get('period_type', 'created')
        f_date_from = request.args.get('date_from', '')
        f_date_to = request.args.get('date_to', '')

        # Build API filter params
        api_params = {}
        if f_pipeline:
            api_params['filter[pipeline_id][]'] = int(f_pipeline)
        if f_statuses:
            api_params['filter[statuses][]'] = [int(s) for s in f_statuses]

        ts_from = None
        ts_to = None
        if f_date_from:
            ts_from = int(datetime.strptime(f_date_from, '%Y-%m-%d').timestamp())
        if f_date_to:
            ts_to = int((datetime.strptime(f_date_to, '%Y-%m-%d') + timedelta(days=1)).timestamp()) - 1

        if f_period_type == 'created':
            if ts_from:
                api_params['filter[created_at][from]'] = ts_from
            if ts_to:
                api_params['filter[created_at][to]'] = ts_to
        elif f_period_type == 'closed':
            if ts_from:
                api_params['filter[closed_at][from]'] = ts_from
            if ts_to:
                api_params['filter[closed_at][to]'] = ts_to
            if not f_statuses:
                api_params['filter[statuses][]'] = [STATUS_WON, STATUS_LOST]

        # Fetch all leads
        leads = kommo.get_all_leads(params=api_params)

        # ─── KPIs ───
        total_leads = len(leads)
        total_value = sum(lead.get('price', 0) or 0 for lead in leads)
        won_leads_list = [l for l in leads if l.get('status_id') == STATUS_WON]
        lost_leads_list = [l for l in leads if l.get('status_id') == STATUS_LOST]
        won_count = len(won_leads_list)
        lost_count = len(lost_leads_list)
        won_value = sum(l.get('price', 0) or 0 for l in won_leads_list)
        ticket_medio = (won_value / won_count) if won_count > 0 else 0
        conversion_rate = (won_count / total_leads * 100) if total_leads > 0 else 0
        loss_rate = (lost_count / total_leads * 100) if total_leads > 0 else 0

        # ─── Leads by status (funnel chart) ───
        status_counts = defaultdict(int)
        for lead in leads:
            sid = lead.get('status_id', 0)
            name = status_map.get(sid, f'Status {sid}')
            status_counts[name] += 1
        funnel_labels = list(status_counts.keys())
        funnel_values = list(status_counts.values())

        # ─── Source analysis ───
        source_counts = defaultdict(int)
        source_won = defaultdict(int)
        source_value = defaultdict(float)
        for lead in leads:
            src = 'Nao preenchido'
            if source_field_id:
                vals = get_custom_field_values(lead, source_field_id)
                if vals:
                    src = vals[0]
            source_counts[src] += 1
            if lead.get('status_id') == STATUS_WON:
                source_won[src] += 1
                source_value[src] += lead.get('price', 0) or 0
        source_labels = list(source_counts.keys())
        source_values = list(source_counts.values())

        # Source conversion table
        source_table = []
        for src in source_counts:
            cnt = source_counts[src]
            won = source_won.get(src, 0)
            val = source_value.get(src, 0)
            conv = (won / cnt * 100) if cnt > 0 else 0
            source_table.append({
                'name': src, 'leads': cnt, 'won': won,
                'value': val, 'conversion': conv,
            })
        source_table.sort(key=lambda x: x['leads'], reverse=True)

        # ─── Lead type analysis ───
        type_counts = defaultdict(int)
        for lead in leads:
            lt = 'Nao preenchido'
            if lead_type_field_id:
                vals = get_custom_field_values(lead, lead_type_field_id)
                if vals:
                    lt = vals[0]
            type_counts[lt] += 1
        type_labels = list(type_counts.keys())
        type_values = list(type_counts.values())

        # ─── Leads by responsible ───
        responsible_counts = defaultdict(int)
        responsible_won = defaultdict(int)
        for lead in leads:
            uid = lead.get('responsible_user_id', 0)
            name = users.get(uid, f'User {uid}')
            responsible_counts[name] += 1
            if lead.get('status_id') == STATUS_WON:
                responsible_won[name] += 1
        responsible_labels = list(responsible_counts.keys())
        responsible_values = list(responsible_counts.values())

        # ─── Loss reasons ───
        loss_reason_counts = defaultdict(int)
        for lead in lost_leads_list:
            embedded = lead.get('_embedded', {})
            loss_reason = embedded.get('loss_reason')
            if loss_reason:
                reason_name = loss_reason[0].get('name', 'Sem motivo') if isinstance(loss_reason, list) else loss_reason.get('name', 'Sem motivo')
            else:
                reason_name = 'Sem motivo'
            loss_reason_counts[reason_name] += 1
        loss_labels = list(loss_reason_counts.keys())
        loss_values = list(loss_reason_counts.values())

        # ─── City analysis ───
        city_counts = defaultdict(int)
        for lead in leads:
            city = 'Nao preenchido'
            if city_field_id:
                vals = get_custom_field_values(lead, city_field_id)
                if vals:
                    city = vals[0]
            city_counts[city] += 1
        city_labels = list(city_counts.keys())
        city_values = list(city_counts.values())

        # ─── Sonorização analysis ───
        sound_counts = defaultdict(int)
        for lead in leads:
            snd = 'Nao preenchido'
            if sound_field_id:
                vals = get_custom_field_values(lead, sound_field_id)
                if vals:
                    snd = vals[0]
            sound_counts[snd] += 1
        sound_labels = list(sound_counts.keys())
        sound_values = list(sound_counts.values())

        # ─── Momentos de Interesse analysis ───
        interest_counts = defaultdict(int)
        for lead in leads:
            mi = 'Nao preenchido'
            if interest_field_id:
                vals = get_custom_field_values(lead, interest_field_id)
                if vals:
                    mi = vals[0]
            interest_counts[mi] += 1
        interest_labels = list(interest_counts.keys())
        interest_values = list(interest_counts.values())

        # ─── Decisores analysis ───
        decisor_counts = defaultdict(int)
        for lead in leads:
            dec = 'Nao preenchido'
            if decisor_field_id:
                vals = get_custom_field_values(lead, decisor_field_id)
                if vals:
                    dec = vals[0]
            decisor_counts[dec] += 1
        decisor_labels = list(decisor_counts.keys())
        decisor_values = list(decisor_counts.values())

        # ─── Recent leads for table ───
        recent_leads = []
        for lead in leads[:50]:
            created = lead.get('created_at', 0)
            created_str = datetime.fromtimestamp(created).strftime('%d/%m/%Y') if created else '-'
            closed = lead.get('closed_at', 0)
            closed_str = datetime.fromtimestamp(closed).strftime('%d/%m/%Y') if closed else '-'

            src = '-'
            if source_field_id:
                vals = get_custom_field_values(lead, source_field_id)
                if vals:
                    src = vals[0]

            city = '-'
            if city_field_id:
                vals = get_custom_field_values(lead, city_field_id)
                if vals:
                    city = vals[0]

            venue = '-'
            if venue_field_id:
                vals = get_custom_field_values(lead, venue_field_id)
                if vals:
                    venue = vals[0]

            event_date = '-'
            if event_date_field_id:
                vals = get_custom_field_values(lead, event_date_field_id)
                if vals and vals[0]:
                    try:
                        ts = int(vals[0])
                        event_date = datetime.fromtimestamp(ts).strftime('%d/%m/%Y')
                    except (ValueError, TypeError):
                        event_date = str(vals[0])

            recent_leads.append({
                'name': lead.get('name', 'Sem nome'),
                'price': lead.get('price', 0) or 0,
                'status': status_map.get(lead.get('status_id', 0), '-'),
                'status_id': lead.get('status_id', 0),
                'responsible': users.get(lead.get('responsible_user_id', 0), '-'),
                'source': src,
                'city': city,
                'venue': venue,
                'event_date': event_date,
                'created_at': created_str,
                'closed_at': closed_str,
            })

        # ─── Filter options ───
        pipeline_options = []
        for pid, pdata in pipelines.items():
            if pdata.get('is_archive'):
                continue
            pipeline_options.append({'id': pid, 'name': pdata['name']})

        status_options = []
        if f_pipeline and int(f_pipeline) in pipelines:
            for sid, sdata in pipelines[int(f_pipeline)]['statuses'].items():
                status_options.append({'id': sid, 'name': sdata['name'], 'sort': sdata['sort']})
        else:
            seen = set()
            for pid, pdata in pipelines.items():
                if pdata.get('is_archive'):
                    continue
                for sid, sdata in pdata['statuses'].items():
                    if sid not in seen:
                        status_options.append({'id': sid, 'name': sdata['name'], 'sort': sdata['sort']})
                        seen.add(sid)
        status_options.sort(key=lambda x: x['sort'])

        return render_template('dashboard.html',
                               authenticated=True,
                               account_name=account_name,
                               # KPIs
                               total_leads=total_leads,
                               total_value=total_value,
                               won_count=won_count,
                               lost_count=lost_count,
                               won_value=won_value,
                               ticket_medio=ticket_medio,
                               conversion_rate=conversion_rate,
                               loss_rate=loss_rate,
                               # Funnel
                               funnel_labels=funnel_labels,
                               funnel_values=funnel_values,
                               # Source
                               source_labels=source_labels,
                               source_values=source_values,
                               source_table=source_table,
                               # Lead type
                               type_labels=type_labels,
                               type_values=type_values,
                               # Responsible
                               responsible_labels=responsible_labels,
                               responsible_values=responsible_values,
                               # Loss reasons
                               loss_labels=loss_labels,
                               loss_values=loss_values,
                               # Custom fields
                               city_labels=city_labels,
                               city_values=city_values,
                               sound_labels=sound_labels,
                               sound_values=sound_values,
                               interest_labels=interest_labels,
                               interest_values=interest_values,
                               decisor_labels=decisor_labels,
                               decisor_values=decisor_values,
                               # Table
                               recent_leads=recent_leads,
                               # Filters
                               pipeline_options=pipeline_options,
                               status_options=status_options,
                               f_pipeline=f_pipeline,
                               f_statuses=f_statuses,
                               f_period_type=f_period_type,
                               f_date_from=f_date_from,
                               f_date_to=f_date_to,
                               STATUS_WON=STATUS_WON,
                               STATUS_LOST=STATUS_LOST)
    except Exception as e:
        return render_template('error.html', authenticated=True,
                               error_message=f"Erro ao carregar dados: {e}")


@app.route('/api/statuses')
def api_statuses():
    if not kommo.is_authenticated():
        return jsonify({'error': 'not authenticated'}), 401
    pipeline_id = request.args.get('pipeline_id', '')
    pipelines, _ = build_pipelines_map()
    statuses = []
    if pipeline_id and int(pipeline_id) in pipelines:
        for sid, sdata in pipelines[int(pipeline_id)]['statuses'].items():
            statuses.append({'id': sid, 'name': sdata['name'], 'sort': sdata['sort']})
    else:
        seen = set()
        for pid, pdata in pipelines.items():
            if pdata.get('is_archive'):
                continue
            for sid, sdata in pdata['statuses'].items():
                if sid not in seen:
                    statuses.append({'id': sid, 'name': sdata['name'], 'sort': sdata['sort']})
                    seen.add(sid)
    statuses.sort(key=lambda x: x['sort'])
    return jsonify({'statuses': statuses})


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
