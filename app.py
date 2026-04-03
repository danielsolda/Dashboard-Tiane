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


# ─── Shared helpers ───

def discover_custom_fields():
    """Discover all custom field IDs used across dashboards."""
    custom_fields_data = kommo.get_custom_fields('leads')
    fields = {}
    for key, search in [('source', 'fonte'), ('lead_type', 'tipo'), ('interest', 'momento'),
                         ('city', 'cidade'), ('sound', 'sonori'), ('decisor', 'decisor'),
                         ('venue', 'local'), ('event_date', 'data do evento')]:
        result = find_custom_field_id(custom_fields_data, search)
        fields[key] = result[0] if result else None
    return fields


def extract_field(lead, field_id, default='-'):
    if not field_id:
        return default
    vals = get_custom_field_values(lead, field_id)
    return vals[0] if vals else default


def count_by_field(leads, field_id, default='Nao preenchido'):
    counts = defaultdict(int)
    for lead in leads:
        counts[extract_field(lead, field_id, default)] += 1
    return counts


def build_filter_options(pipelines, f_pipeline=''):
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
    return pipeline_options, status_options


def build_leads_table(leads, fields, status_map, users):
    recent_leads = []
    for lead in leads[:50]:
        created = lead.get('created_at', 0)
        created_str = datetime.fromtimestamp(created).strftime('%d/%m/%Y') if created else '-'
        closed = lead.get('closed_at', 0)
        closed_str = datetime.fromtimestamp(closed).strftime('%d/%m/%Y') if closed else '-'

        event_date = '-'
        if fields['event_date']:
            vals = get_custom_field_values(lead, fields['event_date'])
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
            'source': extract_field(lead, fields['source']),
            'city': extract_field(lead, fields['city']),
            'venue': extract_field(lead, fields['venue']),
            'event_date': event_date,
            'created_at': created_str,
            'closed_at': closed_str,
        })
    return recent_leads


# ─── Dashboard route (Leads criados) ───

@app.route('/dashboard')
def dashboard():
    if not kommo.is_authenticated():
        return redirect(url_for('login'))

    try:
        account = kommo.get_account()
        account_name = account.get('name', 'Kommo CRM')

        pipelines, status_map = build_pipelines_map()
        users = build_users_map()
        fields = discover_custom_fields()

        # ─── Parse filters (created_at only) ───
        f_pipeline = request.args.get('pipeline', '')
        f_statuses = request.args.getlist('statuses')
        f_date_from = request.args.get('date_from', '')
        f_date_to = request.args.get('date_to', '')

        api_params = {}
        if f_pipeline:
            api_params['filter[pipeline_id][]'] = int(f_pipeline)
        if f_statuses:
            api_params['filter[statuses][]'] = [int(s) for s in f_statuses]
        if f_date_from:
            api_params['filter[created_at][from]'] = int(datetime.strptime(f_date_from, '%Y-%m-%d').timestamp())
        if f_date_to:
            api_params['filter[created_at][to]'] = int((datetime.strptime(f_date_to, '%Y-%m-%d') + timedelta(days=1)).timestamp()) - 1

        leads = kommo.get_all_leads(params=api_params)

        # ─── KPIs ───
        total_leads = len(leads)
        total_value = sum(lead.get('price', 0) or 0 for lead in leads)
        won_count = len([l for l in leads if l.get('status_id') == STATUS_WON])
        lost_count = len([l for l in leads if l.get('status_id') == STATUS_LOST])
        open_count = total_leads - won_count - lost_count

        # ─── Funnel ───
        status_counts = defaultdict(int)
        for lead in leads:
            status_counts[status_map.get(lead.get('status_id', 0), f'Status {lead.get("status_id", 0)}')] += 1
        funnel_labels = list(status_counts.keys())
        funnel_values = list(status_counts.values())

        # ─── Source ───
        source_counts = count_by_field(leads, fields['source'])
        source_labels = list(source_counts.keys())
        source_values = list(source_counts.values())

        # ─── Lead type ───
        type_counts = count_by_field(leads, fields['lead_type'])
        type_labels = list(type_counts.keys())
        type_values = list(type_counts.values())

        # ─── City ───
        city_counts = count_by_field(leads, fields['city'])
        city_labels = list(city_counts.keys())
        city_values = list(city_counts.values())

        # ─── Sonorização ───
        sound_counts = count_by_field(leads, fields['sound'])
        sound_labels = list(sound_counts.keys())
        sound_values = list(sound_counts.values())

        # ─── Momentos de Interesse ───
        interest_counts = count_by_field(leads, fields['interest'])
        interest_labels = list(interest_counts.keys())
        interest_values = list(interest_counts.values())

        # ─── Decisores ───
        decisor_counts = count_by_field(leads, fields['decisor'])
        decisor_labels = list(decisor_counts.keys())
        decisor_values = list(decisor_counts.values())

        # ─── Responsible ───
        responsible_counts = defaultdict(int)
        for lead in leads:
            responsible_counts[users.get(lead.get('responsible_user_id', 0), f'User {lead.get("responsible_user_id", 0)}')] += 1
        responsible_labels = list(responsible_counts.keys())
        responsible_values = list(responsible_counts.values())

        # ─── Leads table ───
        recent_leads = build_leads_table(leads, fields, status_map, users)

        # ─── Filter options ───
        pipeline_options, status_options = build_filter_options(pipelines, f_pipeline)

        return render_template('dashboard.html',
                               authenticated=True,
                               account_name=account_name,
                               total_leads=total_leads, total_value=total_value,
                               won_count=won_count, lost_count=lost_count,
                               open_count=open_count,
                               funnel_labels=funnel_labels, funnel_values=funnel_values,
                               source_labels=source_labels, source_values=source_values,
                               type_labels=type_labels, type_values=type_values,
                               city_labels=city_labels, city_values=city_values,
                               sound_labels=sound_labels, sound_values=sound_values,
                               interest_labels=interest_labels, interest_values=interest_values,
                               decisor_labels=decisor_labels, decisor_values=decisor_values,
                               responsible_labels=responsible_labels, responsible_values=responsible_values,
                               recent_leads=recent_leads,
                               pipeline_options=pipeline_options, status_options=status_options,
                               f_pipeline=f_pipeline, f_statuses=f_statuses,
                               f_date_from=f_date_from, f_date_to=f_date_to,
                               STATUS_WON=STATUS_WON, STATUS_LOST=STATUS_LOST)
    except Exception as e:
        return render_template('error.html', authenticated=True,
                               error_message=f"Erro ao carregar dados: {e}")


# ─── Vendas / Perdidos route (Fechados) ───

@app.route('/vendas')
def vendas():
    if not kommo.is_authenticated():
        return redirect(url_for('login'))

    try:
        account = kommo.get_account()
        account_name = account.get('name', 'Kommo CRM')

        pipelines, status_map = build_pipelines_map()
        users = build_users_map()
        fields = discover_custom_fields()

        # ─── Parse filters (closed_at only) ───
        f_pipeline = request.args.get('pipeline', '')
        f_date_from = request.args.get('date_from', '')
        f_date_to = request.args.get('date_to', '')

        api_params = {
            'filter[statuses][]': [STATUS_WON, STATUS_LOST],
        }
        if f_pipeline:
            api_params['filter[pipeline_id][]'] = int(f_pipeline)
        if f_date_from:
            api_params['filter[closed_at][from]'] = int(datetime.strptime(f_date_from, '%Y-%m-%d').timestamp())
        if f_date_to:
            api_params['filter[closed_at][to]'] = int((datetime.strptime(f_date_to, '%Y-%m-%d') + timedelta(days=1)).timestamp()) - 1

        leads = kommo.get_all_leads(params=api_params)

        # ─── KPIs ───
        total = len(leads)
        won_leads = [l for l in leads if l.get('status_id') == STATUS_WON]
        lost_leads = [l for l in leads if l.get('status_id') == STATUS_LOST]
        won_count = len(won_leads)
        lost_count = len(lost_leads)
        won_value = sum(l.get('price', 0) or 0 for l in won_leads)
        lost_value = sum(l.get('price', 0) or 0 for l in lost_leads)
        ticket_medio = (won_value / won_count) if won_count > 0 else 0
        conversion_rate = (won_count / total * 100) if total > 0 else 0
        loss_rate = (lost_count / total * 100) if total > 0 else 0

        # ─── Source conversion analysis ───
        source_counts = defaultdict(int)
        source_won = defaultdict(int)
        source_value = defaultdict(float)
        for lead in leads:
            src = extract_field(lead, fields['source'], 'Nao preenchido')
            source_counts[src] += 1
            if lead.get('status_id') == STATUS_WON:
                source_won[src] += 1
                source_value[src] += lead.get('price', 0) or 0

        source_table = []
        for src in source_counts:
            cnt = source_counts[src]
            won = source_won.get(src, 0)
            val = source_value.get(src, 0)
            conv = (won / cnt * 100) if cnt > 0 else 0
            source_table.append({'name': src, 'leads': cnt, 'won': won, 'value': val, 'conversion': conv})
        source_table.sort(key=lambda x: x['leads'], reverse=True)

        # ─── Loss reasons ───
        loss_reason_counts = defaultdict(int)
        for lead in lost_leads:
            embedded = lead.get('_embedded', {})
            loss_reason = embedded.get('loss_reason')
            if loss_reason:
                reason_name = loss_reason[0].get('name', 'Sem motivo') if isinstance(loss_reason, list) else loss_reason.get('name', 'Sem motivo')
            else:
                reason_name = 'Sem motivo'
            loss_reason_counts[reason_name] += 1
        loss_labels = list(loss_reason_counts.keys())
        loss_values = list(loss_reason_counts.values())

        # ─── Responsible won/lost ───
        resp_won_counts = defaultdict(int)
        resp_lost_counts = defaultdict(int)
        resp_won_value = defaultdict(float)
        for lead in leads:
            name = users.get(lead.get('responsible_user_id', 0), f'User {lead.get("responsible_user_id", 0)}')
            if lead.get('status_id') == STATUS_WON:
                resp_won_counts[name] += 1
                resp_won_value[name] += lead.get('price', 0) or 0
            else:
                resp_lost_counts[name] += 1
        all_resp = sorted(set(list(resp_won_counts.keys()) + list(resp_lost_counts.keys())))
        resp_labels = all_resp
        resp_won_values = [resp_won_counts.get(r, 0) for r in all_resp]
        resp_lost_values = [resp_lost_counts.get(r, 0) for r in all_resp]

        # ─── City conversion ───
        city_won = defaultdict(int)
        city_total = defaultdict(int)
        for lead in leads:
            c = extract_field(lead, fields['city'], 'Nao preenchido')
            city_total[c] += 1
            if lead.get('status_id') == STATUS_WON:
                city_won[c] += 1
        city_table = []
        for c in city_total:
            cnt = city_total[c]
            won = city_won.get(c, 0)
            conv = (won / cnt * 100) if cnt > 0 else 0
            city_table.append({'name': c, 'total': cnt, 'won': won, 'conversion': conv})
        city_table.sort(key=lambda x: x['total'], reverse=True)

        # ─── Leads table ───
        recent_leads = build_leads_table(leads, fields, status_map, users)

        # ─── Filter options ───
        pipeline_options, _ = build_filter_options(pipelines, f_pipeline)

        return render_template('vendas.html',
                               authenticated=True,
                               account_name=account_name,
                               total=total, won_count=won_count, lost_count=lost_count,
                               won_value=won_value, lost_value=lost_value,
                               ticket_medio=ticket_medio,
                               conversion_rate=conversion_rate, loss_rate=loss_rate,
                               source_table=source_table,
                               loss_labels=loss_labels, loss_values=loss_values,
                               resp_labels=resp_labels,
                               resp_won_values=resp_won_values,
                               resp_lost_values=resp_lost_values,
                               city_table=city_table,
                               recent_leads=recent_leads,
                               pipeline_options=pipeline_options,
                               f_pipeline=f_pipeline,
                               f_date_from=f_date_from, f_date_to=f_date_to,
                               STATUS_WON=STATUS_WON, STATUS_LOST=STATUS_LOST)
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
