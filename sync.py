#!/usr/bin/env python3
import json
import subprocess
import argparse
from datetime import datetime, timedelta


parser = argparse.ArgumentParser(description='Process some integers.')

thirty_days_ago = datetime.today() - timedelta(days=30)
fifteen_days_ago = datetime.today() - timedelta(days=15)
MID_TIME = fifteen_days_ago.strftime('%Y-%m-%d')
START_TIME = thirty_days_ago.strftime('%Y-%m-%d')
START_DATE = thirty_days_ago
TIME_DELTA = timedelta(days=10)

def FormatDate(d):
  return d.strftime('%Y-%m-%d')

def InvokeGH(cmd):
  print(cmd)
  output = subprocess.check_output(cmd, shell=True)
  return json.loads(output)


PR_COLUMNS = [
		'additions',
		'assignees',
		'author',
		'closed',
		'closedAt',
		'createdAt',
		'deletions',
		'isDraft',
		'labels',
		'mergedAt',
		'mergedBy',
		'number',
		'state',
		'title',
		'updatedAt',
		'url',]


def GetPullRequests(gh_bin, start_date, owner, repo):
  prs = []
  start = start_date
  while start <= datetime.today():
    start_str = FormatDate(start)
    end_str = FormatDate(start + TIME_DELTA)
    prs.extend(InvokeGH(f'{gh_bin} pr list -L 1000 -R {owner}/{repo} -s all -S "created:>={start_str} created:<{end_str}" --json={",".join(PR_COLUMNS)}'))
    print(len(prs))
    start += TIME_DELTA
  for pr in prs:
    pr['repo'] = repo
    pr['owner'] = owner
    pr['author'] = pr['author']['login']
    for k, v in pr.items():
      if isinstance(v, list) or isinstance(v, dict):
        pr[k] = str(v)
  return prs


def GetRepos(gh_bin, owner):
  repos = InvokeGH('{} repo list -L 1000 {} --json=name'.format(gh_bin, owner))
  return (x['name'] for x in repos)


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='github PR sync')
  parser.add_argument('--owners', type=str, nargs='?', default='tidbcloud')
  parser.add_argument('--gh_bin', type=str, nargs='?', default='gh')
  parser.add_argument('--out', type=str, nargs='?', default='/tmp/result.json')
  args = parser.parse_args()
  result = []
  for owner in args.owners.strip().split(','):
    repos = list(GetRepos(args.gh_bin, owner))
    for repo in repos:
      print(owner, repo)
      result.extend(GetPullRequests(args.gh_bin, START_DATE, owner, repo))
      print(len(result))
  with open(args.out, 'w') as fp:
    for x in result:
      json.dump(x, fp)
      fp.write('\n')

