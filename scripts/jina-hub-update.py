""" Script to change versioning of files (eg. manifest.yml) for executors
[encoders, crafters, indexers, rankers, evaluators, classifiers etc.].
It also adds the required jina version.
Commits the change in the branch and raises a PR for the executor.
Then attempts to automatically merge the PRs
"""
import glob
import os
import sys
import time
import traceback
from typing import List, Optional

import git
import requests
import semver
from github import Github, PullRequestMergeStatus
from github.PullRequest import PullRequest
from ruamel.yaml import YAML

WAIT_BETWEEN_PR_CHECKS = 5 * 60

# this one has PR push access
g = Github(os.environ["GITHUB_TOKEN"])

yaml = YAML()


def handle_module(fpath, jina_core_version, hub_repo, hub_origin, gh_hub_repo) -> Optional[PullRequest]:
    """for each module with manifest.yml attempts to bump version and add jina core version.
    then opens PR"""
    pr = None
    dname = fpath.split('/')[-2]
    print(f'handling {dname}...')
    with open(fpath) as fp:
        info = yaml.load(fp)
        # make sure the (possibly) existing version is older
        if 'jina_version' in info.keys():
            existing_jina_version = info['jina_version']
            if semver.VersionInfo.parse(existing_jina_version) >= semver.VersionInfo.parse(jina_core_version):
                print(f'existing jina-core version for {dname} was greater or equal than version to update '
                      f'({existing_jina_version} >= {jina_core_version}). Skipping...')
                return

        old_ver = info['version']
        new_ver = '.'.join(old_ver.split('.')[:-1] + [str(int(old_ver.split('.')[-1]) + 1)])
        info['version'] = new_ver
        print(f'bumped to {new_ver}')
        info['jina_version'] = jina_core_version
    with open(fpath, 'w') as fp:
        yaml.dump(info, fp)

    br_name = ''
    try:
        print('preparing the branch ...')
        br_name = f'chore-{dname.lower()}-{new_ver.replace(".", "-")}-core-{jina_core_version.replace(".", "-")}'
        new_branch = hub_repo.create_head(br_name)
        new_branch.checkout()

        print(f'bumping version to {new_ver} and committing to {new_branch}...')
        hub_repo.git.add(update=True)
        hub_repo.index.commit(f'chore: bump {dname} version to {new_ver} (jina core: {jina_core_version})')
        hub_repo.git.push('--set-upstream', hub_origin, hub_repo.head.ref)

        print('making a PR ...')
        title_string = f'bumping version for {dname} to {new_ver} (new jina: {jina_core_version})'
        body_string = f'Due to the release of jina core v{jina_core_version}, this PR is automatically submitted to ' \
                      f'ensure compatibility '

        pr = gh_hub_repo.create_pull(
            title=title_string,
            body=body_string,
            head=br_name,
            base='master'
        )
    except git.GitCommandError as e:
        print(f'Caught exception: {repr(e)}')
        if 'tip of your current branch is behind' in str(e) \
                or 'the remote contains work that you do' in str(e):
            print(f'warning: Branch "{br_name}" already existed. Attempting to get pr from GH...')
            prs = list(gh_hub_repo.get_pulls(
                head=f'jina-ai/jina-hub:{br_name}',
                state='open'
            ))
            if len(prs) == 1:
                pr = prs[0]
                return pr
            print(f'Couldn\'t retrieve PR for branch. Skipping...')
    except Exception:
        raise
    finally:
        hub_repo.git.checkout('master')
        if br_name:
            hub_repo.delete_head(br_name, force=True)

    return pr


def all_checks_passed(pr: PullRequest, sha: str) -> Optional[bool]:
    """
    attempts to check whether all checks from a PR head ref have completed and passed

    :param pr: the PullRequest object
    :param sha: the sha of the tip of the PullRequest object (PyGithub) :return: None if at least one of the checks
    hasn't completed, False if they have all completed and at least one has failed. otherwise returns True
    """
    result = requests.get(
        f'https://api.github.com/repos/jina-ai/jina-hub/commits/{sha}/check-runs',
        headers={'Accept': 'application/vnd.github.v3+json'}
    )
    checks = result.json()
    runs = checks['check_runs']
    print(f'Got {len(runs)} runs to check for PR: \n{[(r["name"], r["status"], r["conclusion"]) for r in runs]}')
    for c in runs:
        if c['status'] == 'completed':
            if c['conclusion'] == 'failure':
                # make comment
                pr.create_issue_comment(
                    "@jina-ai/engineering One of the checks is failing in this PR"
                )
                return False
        else:
            return None
    return True


def comment_fail(pr, br_name):
    pr.create_issue_comment(
        "@jina-ai/engineering Automatic merge failed. Please investigate"
    )
    print(f'Merge of {br_name} failed. Check {pr.html_url}')
    sys.exit(1)


def handle_prs(prs: List[PullRequest]):
    """
    traverses list of open PRs. Confirms whether checks have passed or not. If they have, merges. If not,
    either tries again or, if they have failed, removes them and comments on the PR :param prs: :return: None when done
    """
    # noinspection PyBroadException
    try:
        # allow for checks to be initiated. It's not instantaneous
        print(f'waiting for 30 secs. before continuing...')
        time.sleep(30)
        new_prs = []
        while len(prs) > 0:
            for i, pr in enumerate(prs):
                print(f'Checking PR {pr} ( {pr.html_url} )...')
                br_name = pr.head.ref
                last_commit = sorted(list(pr.get_commits()), key=lambda t: t.commit.author.date)[-1]
                sha = last_commit.sha
                checks_passed = all_checks_passed(pr, sha)
                if checks_passed is None:
                    print(f'Not all checks have completed for {br_name}. Skipping and will attempt later...')
                    new_prs.append(pr)
                else:
                    if checks_passed:
                        print(f'All checks completed and passed for {br_name}. Attempting to merge...')
                        # this should work with the DEV BOT TOKEN (as it has root access to all)
                        try:
                            status: PullRequestMergeStatus = pr.merge('automatic merge')
                            print(f'status after merge: {status}')
                        except Exception as e:
                            print(repr(e))
                            comment_fail(pr, br_name)
                    else:
                        print(f'warning: not all checks have passed for {br_name}. Will abandon trying.')

            # starting the checking process again on the subset
            # of PRs that had not yet completed
            prs = new_prs
            print(f'Have {len(prs)} PRs left to check')
            if len(prs) > 0:
                print(f'waiting for {WAIT_BETWEEN_PR_CHECKS // 60} mins. before continuing...')
                time.sleep(WAIT_BETWEEN_PR_CHECKS)
            new_prs = []
        print('Done!')
        return
    except Exception:
        print(f'Error occurred: {traceback.format_exc()}')
    return


def main():
    hub_repo = git.Repo('jina-hub')
    hub_origin = hub_repo.remote(name='origin')
    hub_origin_url = list(hub_origin.urls)[0]
    assert 'jina-ai/jina-hub' in hub_origin_url, f'hub repo was not initialized correctly'
    gh_hub_repo = g.get_repo('jina-ai/jina-hub')

    jina_core_repo = git.Repo('.')
    core_origin_url = list(jina_core_repo.remote(name='origin').urls)[0]
    assert 'jina-ai/jina' in core_origin_url, f'core repo was not initialized correctly'

    # make sure to sort them correctly. default is alphabetically
    tags = sorted(jina_core_repo.tags, key=lambda t: t.commit.committed_datetime)
    print(f'tags = {tags}')
    print(f'latest tag = {tags[-1].tag.tag}')
    jina_core_version = tags[-1].tag.tag[1:]  # remove leading 'v'
    print(f'got jina core v: "{jina_core_version}"')

    print(f'cur. dir. is "{os.getcwd()}"')

    modules = glob.glob(f'jina-hub/**/manifest.yml', recursive=True)
    print(f'got {len(modules)} modules to update')

    prs: List[PullRequest] = []

    # traverse list of modules in jina-hub
    for fpath in modules:
        pr = handle_module(fpath, jina_core_version, hub_repo, hub_origin, gh_hub_repo)
        if pr:
            prs.append(pr)

    handle_prs(prs)


if __name__ == '__main__':
    main()
