import urllib.request
import json
import sys

def main():
    url = "https://api.github.com/repos/carnat/yahoo-finance-mcp/pulls?state=all&per_page=10"
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0"}
    )
    try:
        with urllib.request.urlopen(req) as response:
            prs = json.loads(response.read().decode('utf-8'))
            for pr in prs:
                print(f"#{pr['number']} ({pr['state']}): {pr['title']} (Branch: {pr['head']['ref']})")
    except Exception as e:
        print(f"Error fetching PRs: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
