#!/usr/bin/env python3
import json
import argparse
from datetime import datetime
from github import Github
import os
import re
from retry import retry
import time
import collections
from dataclasses import dataclass


@dataclass
class Context:
  excluded_users : dict
  client : Github
  output_fp : ...
  repo : str = ''
  owner : str = ''


@retry(tries=3, delay=2)
def GetRepos(g, owner):
  result = []
  for repo in g.get_organization(owner).get_repos():
    result.append(repo)
  return result

@retry(tries=3, delay=2)
def GetReviews(pr):
  rs = []
  for r in pr.get_reviews():
    rs.append({
      'user' : r.user.login,
      'state' : r.state,
      'submittedAt' : ToISO(r.submitted_at),
      })
  return rs


@retry(tries=3, delay=2)
def ToObject(ctx, pr):
  now = datetime.utcnow()
  return {
      'id' : pr.id,
      'recordTimestamp' : ToISO(now),
      'additions' : pr.additions,
      'deletions' : pr.deletions,
      'author' : pr.user.login,
      'state' : pr.state,
      'createdAt' : ToISO(pr.created_at),
      'updatedAt' : ToISO(pr.updated_at),
      'closedAt' : ToISO(pr.closed_at),
      'title' : pr.title,
      'url' : ConvertUrl(pr.url),
      'body' : pr.body,
      'reviews' : GetReviews(pr),
      'repo' : ctx.repo,
      'owner' : ctx.owner,
      }

def RateLimit(ctx):
  remaining, _ = ctx.client.rate_limiting
  print('rate limiting remaining', remaining)
  while remaining < 200:
    time.sleep(60)
    remaining, _ = ctx.client.rate_limiting
    print('rate limiting remaining', remaining)


@retry(tries=3, delay=2)
def WritePullRequests(ctx, repo):
  for pr in repo.get_pulls(state='all'):
    print(pr.number)
    RateLimit(ctx)
    if pr.user.login in ctx.excluded_users:
      continue
    o = ToObject(ctx, pr)
    json.dump(o, ctx.output_fp)
    ctx.output_fp.write('\n')

def ToISO(d):
  if not d:
    return None
  return d.isoformat() + 'Z'

def ConvertUrl(url):
  '''https://api.github.com/repos/tidbcloud/infra-api/pulls/860'''
  return re.sub('https://api.github.com/repos/([^/]+)/([^/]+)/pulls/([0-9]+)',
      r'https://github.com/\1/\2/pull/\3',
      url)

if __name__ == '__main__':
  #from github import enable_console_debug_logging
  #enable_console_debug_logging()
  parser = argparse.ArgumentParser(description='github PR sync')
  parser.add_argument('--owners', type=str, nargs='?', default='tidbcloud')
  parser.add_argument('--out', type=str, nargs='?', default='/tmp/result.jsonl')
  parser.add_argument('--exclude_users', type=str, nargs='?', default='tidbcloud-bot,github-actions,ti-srebot,ti-chi-bot,dependabot')
  args = parser.parse_args()
  g = Github(os.environ['GITHUB_TOKEN'])
  with open(args.out, 'w') as fp:
    ctx = Context(
        excluded_users=args.exclude_users.strip().split(','),
        client=g,
        output_fp=fp
        )
    for owner in args.owners.strip().split(','):
      repos = GetRepos(g, owner)
      for repo in repos:
        print(owner, repo.name)
        ctx.repo = repo.name
        ctx.owner = owner
        WritePullRequests(ctx, repo)
