#!/usr/bin/env python3
import json
import argparse
from datetime import datetime
import os
import re
import time
from dataclasses import dataclass
import requests
import copy

class GHClient:
  def __init__(self, token):
    self.token = token
    self.session = requests.Session()
    self.r_count = 0

  def _RateLimit(self):
    self.r_count += 1
    if self.r_count % 10 != 0:
      return
    remaining = self.GetRateLimit()['remaining']
    print('rate limiting remaining', remaining)
    while remaining < 100:
      time.sleep(60)
      remaining = self.GetRateLimit()['remaining']
      print('rate limiting remaining', remaining)


  def _Get(self, path, params={}):
    self._RateLimit()
    return self._GetNoDelay(path, params)


  def _GetNoDelay(self, path, params={}):
    headers = {
        'Accept' : 'application/vnd.github+json',
        'Authorization' : f'token {self.token}',
        }
    url = f'https://api.github.com{path}'
    r = self.session.get(url, headers=headers, params=params)
    print('GET', r.url)
    return r.json()

  def _ListMultiPage(self, path, params={}):
    result = []
    page = 1
    p = copy.copy(params)
    while True:
      p['per_page'] = 100
      p['page'] = page
      r = self._Get(path, p)
      result.extend(r)
      if len(r) < 100:
        break
      page += 1
    return result

  def ListRepos(self, owner):
    return self._ListMultiPage(f'/orgs/{owner}/repos', {'type' : 'all'})

  def ListPulls(self, owner, repo):
    return self._ListMultiPage(f'/repos/{owner}/{repo}/pulls', {'state' : 'all'})

  def GetPull(self, owner, repo, number):
    return self._Get(f'/repos/{owner}/{repo}/pulls/{number}')

  def GetReviews(self, owner, repo, number):
    return self._ListMultiPage(f'/repos/{owner}/{repo}/pulls/{number}/reviews')

  def GetRateLimit(self):
    return self._GetNoDelay('/rate_limit')['resources']['core']


@dataclass
class Context:
  excluded_users : dict
  gh : GHClient
  output_fp : ...
  repo : str = ''
  owner : str = ''
  known : dict = None


def GetRepos(ctx, owner):
  return ctx.gh.ListRepos(owner)

def GetReviews(ctx, pr):
  rs = []
  for r in ctx.gh.GetReviews(ctx.owner, ctx.repo, pr['number']):
    rs.append({
      'user' : r['user']['login'],
      'state' : r['state'],
      'submittedAt' : r['submitted_at'],
      })
  return rs


def ToObject(ctx, pr):
  now = datetime.utcnow()
  return {
      'id' : pr['id'],
      'recordTimestamp' : ToISO(now),
      'additions' : pr['additions'],
      'deletions' : pr['deletions'],
      'author' : pr['user']['login'],
      'state' : pr['state'],
      'createdAt' : pr['created_at'],
      'updatedAt' : pr['updated_at'],
      'closedAt' : pr['closed_at'],
      'title' : pr['title'],
      'url' : ConvertUrl(pr['url']),
      'body' : pr['body'],
      'reviews' : GetReviews(ctx, pr),
      'repo' : ctx.repo,
      'owner' : ctx.owner,
      }


def WritePullRequests(ctx):
  for pr in ctx.gh.ListPulls(ctx.owner, ctx.repo):
    pr_id = pr['id']
    if pr_id in ctx.known and ctx.known[pr_id] == pr['updated_at']:
      print('skip', pr['number'])
      continue
    if pr['user']['login'] in ctx.excluded_users:
      continue
    pr = ctx.gh.GetPull(ctx.owner, ctx.repo, pr['number'])
    o = ToObject(ctx, pr)
    json.dump(o, ctx.output_fp)
    ctx.output_fp.write('\n')

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
    return
  d = {}
  for l in open(progress_file):
    o = json.loads(l)
    i = o['id']
    updated_at = o['updatedAt']
    if (i not in d) or d[i] < updated_at:
      d[i] = updated_at
  return d


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='github PR sync')
  parser.add_argument('--owners', type=str, nargs='?', default='tidbcloud')
  parser.add_argument('--out', type=str, nargs='?', default='/tmp/result.jsonl')
  parser.add_argument('--exclude_users', type=str, nargs='?', default='tidbcloud-bot,github-actions,ti-srebot,ti-chi-bot,dependabot')
  parser.add_argument('--progress_file', type=str, nargs='?', default='')
  args = parser.parse_args()
  gh = GHClient(os.environ['GITHUB_TOKEN'])
  with open(args.out, 'w') as fp:
    ctx = Context(
        excluded_users=args.exclude_users.strip().split(','),
        gh=gh,
        output_fp=fp,
        known=ParseProgress(args.progress_file)
        )
    for owner in args.owners.strip().split(','):
      repos = GetRepos(ctx, owner)
      for repo in repos:
        ctx.repo = repo['name']
        ctx.owner = owner
        WritePullRequests(ctx)
