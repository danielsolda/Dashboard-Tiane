import os
import time
from datetime import datetime, timedelta
from flask import Flask, redirect, url_for, request, render_template, session, flash
from kommo_client import KommoClient
from token_storage import TokenStorage

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-change-me')

storage = TokenStorage()
kommo = KommoClient(token_storage=storage)

# Kommo system status IDs
STATUS_WON = 142
STATUS_LOST = 143


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
    # referer pode vir como "tiane.kommo.com" - extrair só o subdomínio
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


def _build_pipelines_map():
    """Fetch pipelines and build maps for pipeline names and status names/info."""
    pipelines_data = kommo.get_pipelines()
    pipelines = {}  # id -> {name, statuses: {id -> {name, sort, type}}}
    status_map = {}  # status_id -> status_name
    if pipelines_data and '_embedded' in pipelines_data:
        for p in pipelines_data['_embedded'].get('pipelines', []):
            statuses = {}
            if '_embedded' in p:
                for s in p['_embedded'].get('statuses', []):
                    statuses[s['id']] = {
                        'name': s['name'],
                        'sort': s.get('sort', 0),
                        'type': s.get('type', 0),
                    }
                    status_map[s['id']] = s['name']
            pipelines[p['id']] = {
                'name': p['name'],
                'is_archive': p.get('is_archive', False),
                'statuses': statuses,
            }
    return pipelines, status_map


def _build_users_map():
    users_data = kommo.get_users()
    users = {}
    if users_data and '_embedded' in users_data:
        for u in users_data['_embedded'].get('users', []):
            users[u['id']] = u['name']
    return users


@app.route('/dashboard')
def dashboard():
    if not kommo.is_authenticated():
        return redirect(url_for('login'))

    try:
        account = kommo.get_account()
        account_name = account.get('name', 'Kommo CRM')

        pipelines, status_map = _build_pipelines_map()
        users = _build_users_map()

        # --- Parse filters from query params ---
        f_pipeline = request.args.get('pipeline', '')
        f_statuses = request.args.getlist('statuses')
        f_period_type = request.args.get('period_type', 'created')  # created or closed
        f_date_from = request.args.get('date_from', '')
        f_date_to = request.args.get('date_to', '')

        # Convert date strings to timestamps
        ts_from = None
        ts_to = None
        if f_date_from:
            ts_from = int(datetime.strptime(f_date_from, '%Y-%m-%d').timestamp())
        if f_date_to:
            # End of day
            ts_to = int((datetime.strptime(f_date_to, '%Y-%m-%d') + timedelta(days=1)).timestamp()) - 1

        # Build API filter params
        api_pipeline = int(f_pipeline) if f_pipeline else None
        api_statuses = [int(s) for s in f_statuses] if f_statuses else None

        created_from = ts_from if f_period_type == 'created' else None
        created_to = ts_to if f_period_type == 'created' else None
        closed_from = ts_from if f_period_type == 'closed' else None
        closed_to = ts_to if f_period_type == 'closed' else None

        # If period_type is "closed", restrict to won/lost statuses
        if f_period_type == 'closed' and not api_statuses:
            api_statuses = [STATUS_WON, STATUS_LOST]

        # Fetch leads with filters (paginated)
        leads = kommo.get_all_leads(
            pipeline_id=api_pipeline,
            statuses=api_statuses,
            created_from=created_from, created_to=created_to,
            closed_from=closed_from, closed_to=closed_to,
        )

        # Calculate metrics
        total_leads = len(leads)
        total_value = sum(lead.get('price', 0) or 0 for lead in leads)
        won_leads = sum(1 for lead in leads if lead.get('status_id') == STATUS_WON)
        lost_leads = sum(1 for lead in leads if lead.get('status_id') == STATUS_LOST)
        conversion_rate = (won_leads / total_leads * 100) if total_leads > 0 else 0

        # Leads by status (for charts)
        status_counts = {}
        for lead in leads:
            sid = lead.get('status_id', 0)
            name = status_map.get(sid, f'Status {sid}')
            status_counts[name] = status_counts.get(name, 0) + 1

        pipeline_labels = list(status_counts.keys())
        pipeline_values = list(status_counts.values())

        # Recent leads for table
        recent_leads = []
        for lead in leads[:30]:
            created = lead.get('created_at', 0)
            created_str = datetime.fromtimestamp(created).strftime('%d/%m/%Y') if created else '-'
            closed = lead.get('closed_at', 0)
            closed_str = datetime.fromtimestamp(closed).strftime('%d/%m/%Y') if closed else '-'
            recent_leads.append({
                'name': lead.get('name', 'Sem nome'),
                'price': lead.get('price', 0) or 0,
                'status': status_map.get(lead.get('status_id', 0), '-'),
                'status_id': lead.get('status_id', 0),
                'responsible': users.get(lead.get('responsible_user_id', 0), '-'),
                'created_at': created_str,
                'closed_at': closed_str,
            })

        # Build filter options for the template
        pipeline_options = []
        for pid, pdata in pipelines.items():
            if pdata.get('is_archive'):
                continue
            pipeline_options.append({'id': pid, 'name': pdata['name']})

        # Statuses for the selected pipeline (or all if none selected)
        status_options = []
        if api_pipeline and api_pipeline in pipelines:
            for sid, sdata in pipelines[api_pipeline]['statuses'].items():
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
                               total_leads=total_leads,
                               total_value=total_value,
                               won_leads=won_leads,
                               lost_leads=lost_leads,
                               conversion_rate=conversion_rate,
                               pipeline_labels=pipeline_labels,
                               pipeline_values=pipeline_values,
                               status_labels=pipeline_labels,
                               status_values=pipeline_values,
                               recent_leads=recent_leads,
                               # Filter options
                               pipeline_options=pipeline_options,
                               status_options=status_options,
                               # Current filter values
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
    """Return statuses for a given pipeline (AJAX endpoint)."""
    if not kommo.is_authenticated():
        return {'error': 'not authenticated'}, 401
    pipeline_id = request.args.get('pipeline_id', '')
    pipelines, _ = _build_pipelines_map()
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
    return {'statuses': statuses}


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
