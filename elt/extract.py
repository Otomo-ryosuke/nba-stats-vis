import json
import logging
import traceback
from typing import Dict, Optional, Tuple

import functions_framework
import pandas as pd
from google.cloud import storage
from nba_api.stats.endpoints import playercareerstats

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


class NBAStatsProcessor:
    def __init__(self, project_id: str, bucket_name: str):
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.storage_client = storage.Client(project=project_id)

    def get_player_stats(
        self, player_id: int, player_name: str
    ) -> Optional[pd.DataFrame]:
        try:
            career = playercareerstats.PlayerCareerStats(player_id=player_id)
            df = career.get_data_frames()[0]
            df["PLAYER_NAME"] = player_name
            return df
        except Exception as e:
            logger.error(f"選手ID {player_id} のデータ取得エラー: {str(e)}")
            return None

    def upload_to_gcs(self, data: pd.DataFrame, blob_name: str) -> bool:
        try:
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(blob_name)
            csv_data = data.to_csv(index=False)
            blob.upload_from_string(csv_data, content_type="text/csv")
            return True
        except Exception as e:
            logger.error(f"GCSアップロードエラー: {str(e)}")
            return False

    def process_player(self, player_id: int, player_name: str) -> Tuple[bool, str]:
        try:
            df = self.get_player_stats(player_id, player_name)
            if df is None:
                return False, f"選手 {player_name} のデータ取得に失敗しました"

            blob_name = f"player_stats/{player_id}_play_stats.csv"
            if self.upload_to_gcs(df, blob_name):
                return True, f"選手 {player_name} のデータを正常に処理しました"
            return False, f"選手 {player_name} のデータのアップロードに失敗しました"

        except Exception as e:
            return False, f"選手 {player_name} の処理中にエラー発生: {str(e)}"


@functions_framework.http
def process_nba_stats(request) -> Tuple[Dict, int]:
    try:
        request_json = request.get_json()
        if not request_json:
            return {"success": False, "error": "リクエストボディが必要です"}, 400

        project_id = request_json.get("project_id")
        bucket_name = request_json.get("bucket_name")
        players = request_json.get("players", [])

        if not all([project_id, bucket_name, players]):
            return {
                "success": False,
                "error": "project_id, bucket_name, players が必要です",
            }, 400

        processor = NBAStatsProcessor(project_id, bucket_name)
        results = []
        success_count = 0

        for player in players:
            success, message = processor.process_player(
                player.get("id"), player.get("name")
            )
            results.append(
                {
                    "player_id": player.get("id"),
                    "name": player.get("name"),
                    "success": success,
                    "message": message,
                }
            )
            if success:
                success_count += 1

        return {
            "success": True,
            "total_processed": len(players),
            "success_count": success_count,
            "results": results,
        }, 200

    except Exception as e:
        logger.error(f"予期せぬエラーが発生しました: {traceback.format_exc()}")
        return {"success": False, "error": str(e)}, 500


class MockRequest:
    def __init__(self, json_data):
        self._json_data = json_data

    def get_json(self):
        return self._json_data


def print_response(response_data, status_code):
    """レスポンスを整形して出力する"""
    print(f"Status: {status_code}")
    # ensure_ascii=False を使用して日本語を正しく表示
    print(json.dumps(response_data, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    # テストデータ
    test_data = {
        "project_id": "your-project-id",
        "bucket_name": "your-bucket-name",
        "players": [
            {"id": 2544, "name": "LeBron James"},
            {"id": 201939, "name": "Stephen Curry"},
        ],
    }
    mock_request = MockRequest(test_data)

    # 関数の実行
    response, status_code = process_nba_stats(mock_request)
    print_response(response, status_code)
