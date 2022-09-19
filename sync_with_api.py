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

FORMAT = '%(asctime)s %(filename)s:%(lineno)s %(funcName)s %(message)s'
logging.basicConfig(stream=sys.stderr, level=logging.INFO, format=FORMAT)


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


def Execute(cmd):
  logging.info('Execute %s', cmd)
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
    return self._ListMultiPage(f'/repos/{owner}/{repo}/pulls', {'state' : 'all'})

  def GetPull(self, owner, repo, number):
    return self._Get(f'/repos/{owner}/{repo}/pulls/{number}')

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
  def __init__(self, github_client, uploader, known={}, excluded_users=[]):
    self.gh = github_client
    self.known = known
    self.excluded_users = excluded_users
    self.uploader = uploader

  def GetReviews(self, owner, repo, pr):
    rs = []
    for r in self.gh.GetReviews(owner, repo, pr['number']):
      rs.append({
        'user' : r.get('user', {}).get('login')
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
        'author' : pr.get('user', {}).get('login'),
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

  def GetPullRequests(self, owner, repo):
    for pr in self.gh.ListPulls(owner, repo):
      pr_id = pr['id']
      if self.known.get(pr_id, None) == pr['updated_at']:
        continue
      if pr['user']['login'] in self.excluded_users:
        continue
      pr = self.gh.GetPull(owner, repo, pr['number'])
      o = self.ToObject(owner, repo, pr)
      yield o

  def WalkPullRequests(self, repos):
    RL_REMAINING_THRESHOLD = 500
    UPLOAD_BATCH = 200
    prs = []
    for owner, repo in repos:
      logging.info('%s %s', repo, owner)
      rl_hit = False
      for pr in self.GetPullRequests(owner, repo):
        prs.append(pr)
        if len(prs) > UPLOAD_BATCH:
          self.uploader.Upload(prs)
          prs = []
        if self.gh.GetRateLimitRemainingCached() < RL_REMAINING_THRESHOLD:
          logging.warning('rate limit hit, finish for now.')
          rl_hit = True
          break
      if rl_hit:
        break
    if prs:
      self.uploader.Upload(prs)


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='github PR sync')
  parser.add_argument('--selectors', type=str, nargs='?', default='tidbcloud/*')
  parser.add_argument('--out', type=str, nargs='?', default='')
  parser.add_argument('--exclude_users', type=str, nargs='?', default='tidbcloud-bot,github-actions,ti-srebot,ti-chi-bot,dependabot,github-actions[bot]')
  parser.add_argument('--progress_file', type=str, nargs='?', default='')
  parser.add_argument('--bq_table', type=str, nargs='?', default='github.pull_requests_exp')
  parser.add_argument('--bq_schema', type=str, nargs='?', default='schema_extended.json')
  args = parser.parse_args()
  gh = GHClient(os.environ['GITHUB_TOKEN'])
  if args.out:
    logging.info('Uploader: FileUploader')
    uploader = FileUploader(args.out)
  else:
    logging.info('Uploader: BigQueryUploader')
    uploader = BigQueryUploader(args.bq_table, args.bq_schema)
  w = PullRequestWalker(gh, uploader, known=ParseProgress(args.progress_file), excluded_users=args.exclude_users.strip().split(','))
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
