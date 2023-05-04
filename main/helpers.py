import requests
import arrow
import json
from django.conf import settings
from django.urls import reverse
from main.models import SharedNotebook
import re
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from ohapi import api
import logging
from open_humans.models import OpenHumansMember
from django.contrib import messages
from collections import defaultdict
logger = logging.getLogger(__name__)


def get_notebook_files(oh_member_data):
    return [
        i for i in oh_member_data['data'] if i['source'] == 'direct-sharing-71'
    ]


def get_notebook_oh(oh_member_data, notebook_id):
    for data_object in oh_member_data['data']:
        if str(data_object['id']) == notebook_id:
            return (data_object['basename'], data_object['download_url'])


def download_notebook_oh(notebook_url):
    return requests.get(notebook_url).content


def create_notebook_link(notebook, request):
    base_url = request.build_absolute_uri("/").rstrip('/')
    target = request.GET.get('target', '')
    target = "&target=voila" if target == 'voila' else ''
    jupyterhub_url = settings.JUPYTERHUB_BASE_URL
    export_url = reverse('export-notebook', args=(notebook.id,))
    return f'{jupyterhub_url}/notebook-import?notebook_location={base_url}{export_url}&notebook_name={notebook.notebook_name}{target}'


def find_notebook_by_keywords(search_term, search_field=None):
    notebooks_tag = SharedNotebook.objects.filter(
        tags__contains=search_term,
        master_notebook=None)
    if search_field == 'tags':
        return notebooks_tag.order_by('updated_at')
    notebooks_source = SharedNotebook.objects.filter(
                        data_sources__contains=search_term,
                        master_notebook=None)
    if search_field == 'data_sources':
        return notebooks_source.order_by('updated_at')
    notebooks_user = SharedNotebook.objects.filter(
                        oh_member__oh_username__contains=search_term,
                        master_notebook=None)
    if search_field == 'username':
        return notebooks_user.order_by('updated_at')
    notebooks_description = SharedNotebook.objects.filter(
                        description__contains=search_term,
                        master_notebook=None)
    notebooks_name = SharedNotebook.objects.filter(
                        notebook_name__contains=search_term,
                        master_notebook=None)

    nbs = notebooks_tag | notebooks_source | notebooks_description | notebooks_name | notebooks_user
    nbs = nbs.order_by('updated_at')
    return nbs


def suggest_data_sources(notebook_content):
    if potential_sources := re.findall(
        "direct-sharing-\d+", str(notebook_content)
    ):
        response = requests.get(
            'https://www.openhumans.org/api/public-data/members-by-source/')
        results = response.json()['results']
        while response.json()['next']:
            response = requests.get(
              'https://www.openhumans.org/api/public-data/members-by-source/')
            results.append(response.json()['results'])
        source_names = {i['source']: i['name'] for i in results}
        suggested_sources = [source_names[i] for i in potential_sources
                             if i in source_names]
        suggested_sources = list(set(suggested_sources))
        return ",".join(suggested_sources)
    return ""


def identify_master_notebook(notebook_name, oh_member):
    if (
        other_notebooks := SharedNotebook.objects.filter(
            notebook_name=notebook_name
        )
        .exclude(oh_member=oh_member)
        .order_by('created_at')
    ):
        return other_notebooks[0]
    return None


def paginate_items(queryset, page):
    paginator = Paginator(queryset, 10)
    try:
        paged_queryset = paginator.page(page)
    except PageNotAnInteger:
        paged_queryset = paginator.page(1)
    except EmptyPage:
        paged_queryset = paginator.page(paginator.num_pages)
    return paged_queryset


def oh_code_to_member(code):
    """
    Exchange code for token, use this to create and return OpenHumansMember.
    If a matching OpenHumansMember exists, update and return it.
    """
    if settings.OPENHUMANS_CLIENT_SECRET and \
       settings.OPENHUMANS_CLIENT_ID and code:
        data = {
            'grant_type': 'authorization_code',
            'redirect_uri': f'{settings.OPENHUMANS_APP_BASE_URL}/complete',
            'code': code,
        }
        req = requests.post(
            f'{settings.OPENHUMANS_OH_BASE_URL}/oauth2/token/',
            data=data,
            auth=requests.auth.HTTPBasicAuth(
                settings.OPENHUMANS_CLIENT_ID,
                settings.OPENHUMANS_CLIENT_SECRET,
            ),
        )
        data = req.json()

        if 'access_token' in data:
            oh_memberdata = api.exchange_oauth2_member(
                data['access_token'])
            oh_id = oh_memberdata['project_member_id']
            oh_username = oh_memberdata['username']
            try:
                oh_member = OpenHumansMember.objects.get(oh_id=oh_id)
                logger.debug(f'Member {oh_id} re-authorized.')
                oh_member.access_token = data['access_token']
                oh_member.refresh_token = data['refresh_token']
                oh_member.token_expires = OpenHumansMember.get_expiration(
                    data['expires_in'])
            except OpenHumansMember.DoesNotExist:
                oh_member = OpenHumansMember.create(
                    oh_id=oh_id,
                    oh_username=oh_username,
                    access_token=data['access_token'],
                    refresh_token=data['refresh_token'],
                    expires_in=data['expires_in'])
                logger.debug(f'Member {oh_id} created.')
            oh_member.save()

            return oh_member

        elif 'error' in req.json():
            logger.debug(f'Error in token exchange: {req.json()}')
        else:
            logger.warning('Neither token nor error info in OH response!')
    else:
        logger.error('OH_CLIENT_SECRET or code are unavailable')
    return None


def add_notebook_helper(request, notebook_url, notebook_name, oh_member):
    notebook_content = download_notebook_oh(notebook_url)
    notebook, created = SharedNotebook.objects.get_or_create(
                                            oh_member=oh_member,
                                            notebook_name=notebook_name)
    notebook.description = request.POST.get('description')
    tags = request.POST.get('tags')
    tags = [tag.strip() for tag in tags.split(',')]
    notebook.tags = json.dumps(tags)
    data_sources = request.POST.get('data_sources')
    data_sources = [ds.strip() for ds in data_sources.split(',')]
    notebook.data_sources = json.dumps(data_sources)
    notebook.notebook_name = notebook_name
    notebook.notebook_content = notebook_content.decode()
    notebook.updated_at = arrow.now().format()
    notebook.oh_member = oh_member
    notebook.master_notebook = identify_master_notebook(notebook_name,
                                                        oh_member)
    if created:
        notebook.created_at = arrow.now().format()
        messages.info(request, f'Your notebook {notebook_name} has been shared!')
    else:
        messages.info(request, f'Your notebook {notebook_name} has been updated!')
    notebook.save()


def get_all_data_sources_numeric():
    sdict = defaultdict(int)
    for nb in SharedNotebook.objects.filter(master_notebook=None):
        for source in nb.get_data_sources_json():
            sdict[source] += 1
    return sorted(sdict.items(), key=lambda x: x[1], reverse=True)


def get_all_data_sources():
    sorted_sdict = get_all_data_sources_numeric()
    return [i[0] for i in sorted_sdict]
