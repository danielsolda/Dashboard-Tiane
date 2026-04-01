import os
from datetime import datetime
from flask import Flask, redirect, url_for, request, render_template, session, flash
from kommo_client import KommoClient
from token_storage import TokenStorage

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-change-me')

storage = TokenStorage()
kommo = KommoClient(token_storage=storage)


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

    subdomain = request.args.get('referer', kommo.subdomain)
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


@app.route('/dashboard')
def dashboard():
    if not kommo.is_authenticated():
        return redirect(url_for('login'))

    try:
        # Fetch data from Kommo API
        account = kommo.get_account()
        account_name = account.get('name', 'Kommo CRM')

        # Get pipelines and build status map
        pipelines_data = kommo.get_pipelines()
        pipelines = {}
        status_map = {}
        if pipelines_data and '_embedded' in pipelines_data:
            for pipeline in pipelines_data['_embedded'].get('pipelines', []):
                pipelines[pipeline['id']] = pipeline['name']
                if '_embedded' in pipeline:
                    for status in pipeline['_embedded'].get('statuses', []):
                        status_map[status['id']] = status['name']

        # Get users
        users_data = kommo.get_users()
        users = {}
        if users_data and '_embedded' in users_data:
            for user in users_data['_embedded'].get('users', []):
                users[user['id']] = user['name']

        # Get leads
        leads_data = kommo.get_leads()
        leads = []
        if leads_data and '_embedded' in leads_data:
            leads = leads_data['_embedded'].get('leads', [])

        # Calculate metrics
        total_leads = len(leads)
        total_value = sum(lead.get('price', 0) or 0 for lead in leads)
        won_leads = sum(1 for lead in leads if lead.get('status_id') == 142)
        conversion_rate = (won_leads / total_leads * 100) if total_leads > 0 else 0

        # Leads by pipeline status (for funnel chart)
        status_counts = {}
        for lead in leads:
            sid = lead.get('status_id', 0)
            name = status_map.get(sid, f'Status {sid}')
            status_counts[name] = status_counts.get(name, 0) + 1

        pipeline_labels = list(status_counts.keys())
        pipeline_values = list(status_counts.values())

        # Recent leads for table
        recent_leads = []
        for lead in leads[:20]:
            created = lead.get('created_at', 0)
            created_str = datetime.fromtimestamp(created).strftime('%d/%m/%Y') if created else '-'
            recent_leads.append({
                'name': lead.get('name', 'Sem nome'),
                'price': lead.get('price', 0) or 0,
                'status': status_map.get(lead.get('status_id', 0), '-'),
                'responsible': users.get(lead.get('responsible_user_id', 0), '-'),
                'created_at': created_str,
            })

        return render_template('dashboard.html',
                               authenticated=True,
                               account_name=account_name,
                               total_leads=total_leads,
                               total_value=total_value,
                               won_leads=won_leads,
                               conversion_rate=conversion_rate,
                               pipeline_labels=pipeline_labels,
                               pipeline_values=pipeline_values,
                               status_labels=pipeline_labels,
                               status_values=pipeline_values,
                               recent_leads=recent_leads)
    except Exception as e:
        return render_template('error.html', authenticated=True,
                               error_message=f"Erro ao carregar dados: {e}")


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
