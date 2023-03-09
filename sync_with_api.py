#!/usr/bin/env python3
import json
import argparse
import retry
from datetime import datetime
import os
import re
import time
from dataclasses import dataclass
import requests
import copy
import subprocess
import logging
import sys
import pprint
import pymysql

FORMAT = '%(asctime)s %(filename)s:%(lineno)s %(funcName)s %(message)s'
logging.basicConfig(stream=sys.stderr, level=logging.INFO, format=FORMAT)


def ExtractUser(x):
  u = x.get('user')
  if u:
    return u.get('login')
  else:
    return None


def ToISO(d):
  if not d:
    return None
  return d.isoformat() + 'Z'


def ConvertUrl(url):
  return re.sub('https://api.github.com/repos/([^/]+)/([^/]+)/pulls/([0-9]+)',
      r'https://github.com/\1/\2/pull/\3',
      url)


def ParseProgress(progress_file):
  if not progress_file:
    return {}
  d = {}
  for index, l in enumerate(open(progress_file)):
    if index == 0:
      continue
    id_str, updated_at = l.strip().split(',')
    i = int(id_str)
    if (i not in d) or d[i] < updated_at:
      d[i] = updated_at
  return d


def Execute(cmd, need_output=False):
  logging.info('Execute %s', cmd)
  if need_output:
    return subprocess.check_output(cmd, shell=True)
  else:
    return subprocess.check_call(cmd, shell=True)


class GHClient:
  def __init__(self, token):
    self.token = token
    self.session = requests.Session()
    self.r_count = 0
    self.remaining = 0

  @retry.retry(tries=2)
  def _Get(self, path, params={}):
    headers = {
        'Accept' : 'application/vnd.github+json',
        'Authorization' : f'token {self.token}',
        }
    url = f'https://api.github.com{path}'
    r = self.session.get(url, headers=headers, params=params, timeout=10)
    r.raise_for_status()
    logging.info('GET %s', r.url)
    return r.json()

  def _ListMultiPage(self, path, params={}):
    page = 1
    p = copy.copy(params)
    while True:
      p['per_page'] = 100
      p['page'] = page
      r = self._Get(path, p)
      yield from r
      if len(r) < 100:
        break
      page += 1

  def ListRepos(self, owner):
    return self._ListMultiPage(f'/orgs/{owner}/repos', {'type' : 'all'})

  def ListPulls(self, owner, repo):
    return self._ListMultiPage(f'/repos/{owner}/{repo}/pulls', {'state' : 'all', 'sort' : 'updated', 'direction' : 'desc'})

  def GetPull(self, owner, repo, number):
    return self._Get(f'/repos/{owner}/{repo}/pulls/{number}')

  def GetRepo(self, owner, repo):
    return self._Get(f'/repos/{owner}/{repo}')

  def GetReviews(self, owner, repo, number):
    return self._ListMultiPage(f'/repos/{owner}/{repo}/pulls/{number}/reviews')

  def GetRateLimit(self):
    return self._Get('/rate_limit')['resources']['core']

  def GetRateLimitRemainingCached(self):
    self.r_count += 1
    if not self.remaining or self.r_count % 10 == 0:
      self.remaining = self.GetRateLimit()['remaining']
      logging.info('rate limiting remaining %s', self.remaining)
    return self.remaining


class BigQueryUploader:
  def __init__(self, table, schema):
    self.table = table
    self.schema = schema

  def Upload(self, prs):
    with open('/tmp/result_bq.jsonl', 'w') as fp:
      for pr in prs:
        json.dump(pr, fp)
        fp.write('\n')
    Execute(f'bq load --source_format=NEWLINE_DELIMITED_JSON --format=json {self.table} /tmp/result_bq.jsonl {self.schema}')

class MysqlUplaoder:
  def __init__(self, params, create_table_stmt):
    self.create_table_stmt = create_table_stmt
    self.params = params

  def Upload(self, prs):
    if not prs:
      return
    p = self.params
    db_user = p['SQL_USERNAME']
    db_password = p['SQL_PASSWORD']
    db_port = int(p['SQL_PORT'])
    db_address = p['SQL_ADDRESS']
    db_name = p['SQL_DB_NAME']
    cnx = pymysql.connect(user=db_user, password=db_password,
        host=db_address, db=db_name, port=db_port, autocommit=True)
    with cnx.cursor() as cursor:
      cursor.execute(self.create_table_stmt)
      INSERT_STMT = '''
      INSERT INTO pr(pr_id, recordTimestamp, additions, deletions, author, state, createdAt, updatedAt, closedAt, title, url, body, owner, repo, reviews)
      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
      '''
      for pr in prs:
        args = (
            pr['id'],
            pr['recordTimestamp'],
            pr['additions'],
            pr['deletions'],
            pr['author'],
            pr['state'],
            pr['createdAt'],
            pr['updatedAt'],
            pr['closedAt'],
            pr['title'],
            pr['url'],
            '', # pr['body'], save storage space for now.
            pr['owner'],
            pr['repo'],
            json.dumps(pr['reviews'], indent=True)
            )
        cursor.execute(INSERT_STMT, args)


class FileUploader:
  def __init__(self, filename):
    self.filename = filename
    with open(self.filename, 'w') as fp:
      pass

  def Upload(self, prs):
    with open(self.filename, 'a') as fp:
      for pr in prs:
        json.dump(pr, fp)
        fp.write('\n')


class PullRequestWalker:
  def __init__(self, github_client, uploader, known={}, excluded_users=[], gcs_state_file=None):
    self.gh = github_client
    self.known = known
    self.excluded_users = excluded_users
    self.uploader = uploader
    self.gcs_state_file = gcs_state_file
    self.LoadState()

  def LoadState(self):
    if self.gcs_state_file:
      Execute(f'if ! gsutil stat {self.gcs_state_file} ; then echo {{\\"repos\\":{{}}}}|gsutil cp - {self.gcs_state_file}; fi')
      self.state = json.loads(Execute(f'gsutil cat {self.gcs_state_file}', need_output=True).decode('utf8'))
    else:
      self.state = {'repos' : {}}
    pprint.pprint(self.state)

  def SaveState(self):
    if self.gcs_state_file:
      with open('/tmp/state.json', 'w') as f:
        f.write(json.dumps(self.state, indent=True))
      Execute(f'gsutil cp /tmp/state.json {self.gcs_state_file}')


  def GetReviews(self, owner, repo, pr):
    rs = []
    for r in self.gh.GetReviews(owner, repo, pr['number']):
      rs.append({
        'user' : ExtractUser(r),
        'state' : r['state'],
        'submittedAt' : r['submitted_at'],
        })
    return rs

  def ToObject(self, owner, repo, pr):
    now = datetime.utcnow()
    return {
        'id' : pr['id'],
        'recordTimestamp' : ToISO(now),
        'additions' : pr['additions'],
        'deletions' : pr['deletions'],
        'author' : ExtractUser(pr),
        'state' : pr['state'],
        'createdAt' : pr['created_at'],
        'updatedAt' : pr['updated_at'],
        'closedAt' : pr['closed_at'],
        'title' : pr['title'],
        'url' : ConvertUrl(pr['url']),
        'body' : pr['body'],
        'reviews' : self.GetReviews(owner, repo, pr),
        'repo' : repo,
        'owner' : owner,
        }

  def GetRepoLastestPrUpdated(self, owner, repo):
    for pr in self.gh.ListPulls(owner, repo):
      return pr['updated_at']
    return None

  def GetPullRequests(self, owner, repo):
    k = f'{owner}/{repo}'
    repo_updated_at = self.GetRepoLastestPrUpdated(owner, repo)
    last_updated_at = self.state['repos'].get(k, {}).get('updated_at')
    logging.info('%s, updated_at=%s, last_updated_at=%s', k, repo_updated_at, last_updated_at)

    for pr in self.gh.ListPulls(owner, repo):
      pr_id = pr['id']
      if last_updated_at and pr['updated_at'] <= last_updated_at:
        logging.info(f'all new updates are collected, break at {pr_id}')
        break
      if self.known.get(pr_id, None) == pr['updated_at']:
        continue
      if pr['user']['login'] in self.excluded_users:
        continue
      pr = self.gh.GetPull(owner, repo, pr['number'])
      o = self.ToObject(owner, repo, pr)
      yield o
    self.state['repos'].setdefault(k, {})['updated_at'] = repo_updated_at

  def WalkPullRequests(self, repos):
    RL_REMAINING_THRESHOLD = 500
    UPLOAD_BATCH = 200
    prs = []
    def UploadFunc(prs):
      logging.info('uploaded %s records', len(prs))
      self.uploader.Upload(prs)
      self.SaveState()
    for owner, repo in repos:
      rl_hit = False
      for pr in self.GetPullRequests(owner, repo):
        prs.append(pr)
        if len(prs) > UPLOAD_BATCH:
          UploadFunc(prs)
          prs = []
        if self.gh.GetRateLimitRemainingCached() < RL_REMAINING_THRESHOLD:
          logging.warning('rate limit hit, finish for now.')
          rl_hit = True
          break
      if rl_hit:
        break
    if prs:
      UploadFunc(prs)


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='github PR sync')
  parser.add_argument('--selectors', type=str, nargs='?', default='tidbcloud/*')
  parser.add_argument('--out', type=str, nargs='?', default='')
  parser.add_argument('--exclude_users', type=str, nargs='?', default='tidbcloud-bot,github-actions,ti-srebot,ti-chi-bot,dependabot,github-actions[bot]')
  parser.add_argument('--progress_file', type=str, nargs='?', default='')
  parser.add_argument('--bq_table', type=str, nargs='?', default='github.pull_requests_exp')
  parser.add_argument('--bq_schema', type=str, nargs='?', default='schema_extended.json')
  parser.add_argument('--gcs_state_file', type=str, nargs='?', default='state.json')
  parser.add_argument('--mysql', type=bool, nargs='?', default=False)
  args = parser.parse_args()
  gh = GHClient(os.environ['GITHUB_TOKEN'])
  logging.info('Rate-limit: %s', gh.GetRateLimit())
  logging.info(args.mysql)
  if args.out:
    logging.info('Uploader: FileUploader')
    uploader = FileUploader(args.out)
  elif args.mysql:
    logging.info('Uplaoder: MySQL')
    params = os.environ['MYSQL_PARAMS']
    logging.info(params)
    params = dict([x.split('=') for x in params.split(',')])
    uploader = MysqlUplaoder(params, open('create_table.sql').read())
  else:
    logging.info('Uploader: BigQueryUploader')
    uploader = BigQueryUploader(args.bq_table, args.bq_schema)
  w = PullRequestWalker(gh, uploader, known=ParseProgress(args.progress_file), excluded_users=args.exclude_users.strip().split(','), gcs_state_file=args.gcs_state_file)
  repo_pairs = []
  for selector in args.selectors.strip().split(','):
    owner, repo = selector.split('/')
    if repo == '*':
      for r in gh.ListRepos(owner):
        repo_pairs.append((owner, r['name']))
    else:
      repo_pairs.append((owner, repo))
  dedup_set = set()
  unique_repo_pairs = []
  for x in repo_pairs:
    if x in dedup_set:
      continue
    unique_repo_pairs.append(x)
    dedup_set.add(x)
  w.WalkPullRequests(unique_repo_pairs)
