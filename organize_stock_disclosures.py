#!/usr/bin/env python3
"""
株探5年分データを銘柄別に整理するシステム（モード対応版）
- full mode: 5年分全データを処理
- incremental mode: 直近2ヶ月のみ処理して既存データに追加
"""

import json
import boto3
import os
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import logging
import re
from typing import Dict, List, Any, Optional, Tuple
import time

class StockBasedDataOrganizer:
    def __init__(self, mode='incremental'):
        """
        銘柄別データ整理システム初期化
        
        Args:
            mode: 'full' (5年分全て) または 'incremental' (直近2ヶ月のみ)
        """
        self.mode = mode
        
        # AWS認証情報を直接設定
        import os
        aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

        if aws_access_key and aws_secret_key:
            self.s3 = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name="ap-northeast-1"
            )
        else:
            self.s3 = boto3.client('s3', region_name="ap-northeast-1")
        self.bucket_name = "m-s3storage"

        # データ取得パス（正確なパス）
        self.source_prefix = "japan-stocks-5years-chart/monthly-disclosures/"

        # 出力パス
        self.output_prefix = "japan-stocks-5years-chart/stock-based-disclosures/"

        # ログ設定
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

        # 統計
        self.stats = {
            'mode': mode,
            'total_disclosures': 0,
            'new_disclosures': 0,
            'duplicate_disclosures': 0,
            'unique_stocks': 0,
            'processed_months': 0,
            'created_files': 0,
            'updated_files': 0,
            'errors': 0,
            'start_time': None
        }

    def get_target_months(self) -> List[Tuple[int, int]]:
        """対象月を取得（incrementalモード用）"""
        today = datetime.now()
        current_year = today.year
        current_month = today.month

        months = []

        # 前月
        if current_month == 1:
            prev_year = current_year - 1
            prev_month = 12
        else:
            prev_year = current_year
            prev_month = current_month - 1

        months.append((prev_year, prev_month))
        months.append((current_year, current_month))

        return months

    def get_monthly_files_list(self) -> List[str]:
        """S3から月次ファイルリストを取得"""
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=self.source_prefix)

            monthly_files = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        # 月次ファイルの正確な形式: YYYY-MM.json
                        if key.endswith('.json') and re.search(r'/\d{4}-\d{2}\.json$', key):
                            monthly_files.append(key)

            monthly_files.sort()
            return monthly_files
            
        except Exception as e:
            self.logger.error(f"S3ファイル一覧取得エラー: {e}")
            return []

    def load_existing_stock_data(self, stock_code: str) -> Optional[Dict]:
        """既存の銘柄データをS3から読み込み（incrementalモード用）"""
        try:
            key = f"{self.output_prefix}{stock_code}.json"
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            data = json.loads(response['Body'].read().decode('utf-8'))
            return data
        except self.s3.exceptions.NoSuchKey:
            return None
        except Exception as e:
            self.logger.warning(f"既存データ読み込みエラー ({stock_code}): {e}")
            return None

    def create_disclosure_key(self, disclosure: Dict) -> str:
        """開示データのユニークキーを生成（重複判定用）"""
        stock_code = disclosure.get('stock_code', '')
        date = disclosure.get('date_normalized') or disclosure.get('date', '')
        title = disclosure.get('title', '')
        return f"{stock_code}_{date}_{title}"

    def merge_disclosures(self, existing: List[Dict], new: List[Dict]) -> List[Dict]:
        """既存データと新規データをマージ（重複排除）"""
        # 既存データのキーセット作成
        existing_keys = {self.create_disclosure_key(d) for d in existing}
        
        merged = existing.copy()
        new_count = 0
        duplicate_count = 0
        
        for disclosure in new:
            key = self.create_disclosure_key(disclosure)
            if key not in existing_keys:
                merged.append(disclosure)
                existing_keys.add(key)
                new_count += 1
            else:
                duplicate_count += 1
        
        self.stats['new_disclosures'] += new_count
        self.stats['duplicate_disclosures'] += duplicate_count
        
        return merged

    def load_all_monthly_data(self) -> Dict[str, List[Dict]]:
        """全月次データを読み込み、銘柄別に整理"""
        stock_data = defaultdict(list)

        try:
            self.logger.info(f"📦 {self.mode.upper()} MODE: データ読み込み開始")
            
            # モードに応じてファイルリストを決定
            if self.mode == 'full':
                # 全ファイル取得
                monthly_files = self.get_monthly_files_list()
                self.logger.info(f"5年分全データを処理: {len(monthly_files)}ファイル")
            else:
                # 直近2ヶ月のみ
                target_months = self.get_target_months()
                monthly_files = []
                for year, month in target_months:
                    file_key = f"{self.source_prefix}{year}-{month:02d}.json"
                    monthly_files.append(file_key)
                self.logger.info(f"直近2ヶ月のみ処理: {target_months[0][0]}年{target_months[0][1]}月 と {target_months[1][0]}年{target_months[1][1]}月")

            if not monthly_files:
                self.logger.error(f"月次ファイルが見つかりません")
                return {}

            # 各月次ファイルを処理
            for i, file_key in enumerate(monthly_files, 1):
                try:
                    self.logger.info(f"[{i}/{len(monthly_files)}] 処理中: {os.path.basename(file_key)}")

                    # S3からファイル取得
                    response = self.s3.get_object(Bucket=self.bucket_name, Key=file_key)
                    file_content = response['Body'].read().decode('utf-8')
                    data = json.loads(file_content)

                    # データ構造の確認と開示情報の抽出
                    disclosures = []

                    if isinstance(data, dict) and 'disclosures' in data:
                        disclosures = data['disclosures']
                    elif isinstance(data, list):
                        disclosures = data
                    elif isinstance(data, dict) and 'stock_code' in data:
                        disclosures = [data]

                    # 銘柄別に分類
                    processed_count = 0
                    filtered_count = 0
                    for disclosure in disclosures:
                        if not isinstance(disclosure, dict):
                            continue

                        stock_code = disclosure.get('stock_code')
                        if stock_code and re.match(r'^[\dA-Za-z]{4}$', str(stock_code).strip()):
                            stock_code = str(stock_code).strip()

                            # 🔧 修正2: ゴミレコード除外
                            title = disclosure.get('title', '')
                            cn = disclosure.get('company_name', '')
                            if any(kw in title for kw in ['次へ', '前へ', '＞»', '«＜']):
                                filtered_count += 1
                                continue
                            if any(kw in cn for kw in ['次へ', '前へ', '＞»', '«＜']):
                                filtered_count += 1
                                continue
                            if cn == '抽出中' and (len(title) > 100 or '\n' in title):
                                filtered_count += 1
                                continue

                            enhanced_disclosure = self.enhance_disclosure_data(disclosure)
                            stock_data[stock_code].append(enhanced_disclosure)
                            processed_count += 1
                            self.stats['total_disclosures'] += 1

                    skip_info = f" (除外: {filtered_count}件)" if filtered_count > 0 else ""
                    self.logger.info(f"  ✓ 処理完了: {processed_count:,}件{skip_info}")
                    self.stats['processed_months'] += 1

                except Exception as e:
                    self.logger.error(f"ファイル処理エラー: {os.path.basename(file_key)} - {e}")
                    self.stats['errors'] += 1
                    continue

            self.stats['unique_stocks'] = len(stock_data)
            self.logger.info(f"\nデータ読み込み完了:")
            self.logger.info(f"  - 処理ファイル数: {self.stats['processed_months']}")
            self.logger.info(f"  - 読込開示件数: {self.stats['total_disclosures']:,}件")
            self.logger.info(f"  - ユニーク銘柄数: {self.stats['unique_stocks']:,}銘柄")

            return dict(stock_data)

        except Exception as e:
            self.logger.error(f"データ読み込み全体エラー: {e}")
            return {}

    def enhance_disclosure_data(self, disclosure: Dict) -> Dict:
        """開示データの拡張・強化 - 既存データのみ使用"""
        enhanced = disclosure.copy()

        # 基本情報の正規化
        stock_code = enhanced.get('stock_code', '').strip()
        enhanced['stock_code'] = stock_code

        # 既存の会社名をそのまま使用
        company_name = enhanced.get('company_name', '').strip()
        if company_name and company_name not in ['抽出中', '不明', '']:
            enhanced['company_name_cleaned'] = self.normalize_company_name(company_name)
        else:
            enhanced['company_name_cleaned'] = f"銘柄{stock_code}"

        # 日付の正規化
        date_str = enhanced.get('date', '')
        if date_str:
            enhanced['date_normalized'] = self.normalize_date(date_str)
            enhanced['year'] = int(enhanced['date_normalized'][:4]) if enhanced['date_normalized'] else None
            enhanced['month'] = int(enhanced['date_normalized'][5:7]) if enhanced['date_normalized'] else None
            enhanced['quarter'] = self.get_quarter(enhanced['month']) if enhanced['month'] else None

        # カテゴリの詳細分類
        enhanced['category_detailed'] = self.detailed_categorization(
            enhanced.get('title', ''),
            enhanced.get('company_name', '')
        )

        # 重要度スコアの計算
        enhanced['importance_score'] = self.calculate_importance_score(enhanced)

        # 開示タイプの分類
        enhanced['disclosure_type'] = self.classify_disclosure_type(enhanced)

        # タイムスタンプ追加
        enhanced['processed_at'] = datetime.now().isoformat()

        return enhanced

    def normalize_company_name(self, company_name: str) -> str:
        """会社名の正規化"""
        patterns_to_remove = [
            r'\s*\d{2}/\d{2}/\d{2}\s*\d{2}:\d{2}$',
            r'\s*第\d+期.*$',
            r'\s*Notice.*$',
            r'\s*\d+$',
        ]

        normalized = company_name
        for pattern in patterns_to_remove:
            normalized = re.sub(pattern, '', normalized)

        normalized = normalized.replace('(株)', '').replace('㈱', '')
        return normalized.strip()

    def normalize_date(self, date_str: str) -> str:
        """日付の正規化"""
        if not date_str:
            return ''

        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            return date_str

        patterns = [
            (r'^(\d{4})/(\d{1,2})/(\d{1,2})$', lambda m: f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"),
            (r'^(\d{1,2})/(\d{1,2})/(\d{4})$', lambda m: f"{m.group(3)}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"),
            (r'^(\d{4})(\d{2})(\d{2})$', lambda m: f"{m.group(1)}-{m.group(2)}-{m.group(3)}"),
        ]

        for pattern, formatter in patterns:
            match = re.match(pattern, date_str)
            if match:
                try:
                    return formatter(match)
                except ValueError:
                    continue

        return date_str

    def get_quarter(self, month: int) -> int:
        """月から四半期を取得"""
        if month in [1, 2, 3]:
            return 1
        elif month in [4, 5, 6]:
            return 2
        elif month in [7, 8, 9]:
            return 3
        else:
            return 4

    def detailed_categorization(self, title: str, company_name: str = '') -> str:
        """詳細カテゴリ分類"""
        text = f"{title} {company_name}".lower()

        detailed_categories = {
            '決算短信': ['決算短信', '四半期決算短信'],
            '決算説明会': ['決算説明会', '決算briefing', '業績説明会'],
            '業績予想修正': ['業績予想', '業績見通し修正', '業績修正'],
            '配当金': ['配当金', '期末配当', '中間配当', '特別配当'],
            '株主優待': ['株主優待', '優待制度'],
            '自己株式取得': ['自己株式取得', '自社株買い'],
            '株式分割': ['株式分割', '株式併合'],
            '新株予約権': ['新株予約権', 'ストックオプション'],
            '第三者割当増資': ['第三者割当', '第三者割当増資'],
            '代表取締役': ['代表取締役', 'CEO', '社長'],
            '役員人事': ['取締役', '監査役', '執行役員'],
            'M&A買収': ['買収', '株式取得', '子会社化'],
            '業務提携': ['業務提携', '資本提携'],
            '新規事業': ['新規事業', '事業参入'],
            '新製品発表': ['新製品', '新商品'],
            '設備投資': ['設備投資', '工場建設'],
            '適時開示訂正': ['訂正', '修正', '取消'],
            '有価証券報告書': ['有価証券報告書', '四半期報告書'],
            '定時株主総会': ['定時株主総会'],
            '臨時株主総会': ['臨時株主総会'],
        }

        for category, keywords in detailed_categories.items():
            if any(keyword in text for keyword in keywords):
                return category

        return 'その他'

    def calculate_importance_score(self, disclosure: Dict) -> float:
        """重要度スコアの計算（0.0-1.0）"""
        score = 0.0
        title = disclosure.get('title', '').lower()
        category = disclosure.get('category_detailed', '')

        high_importance_categories = [
            '決算短信', '業績予想修正', '代表取締役', 'M&A買収',
            '第三者割当増資'
        ]

        medium_importance_categories = [
            '決算説明会', '配当金', '株式分割', '自己株式取得', '新規事業',
            '業務提携', '設備投資'
        ]

        if category in high_importance_categories:
            score += 0.5
        elif category in medium_importance_categories:
            score += 0.3
        else:
            score += 0.1

        high_impact_keywords = [
            '業績予想修正', '赤字', '黒字転換', '増配', '減配', '無配',
            '買収', '合併'
        ]

        for keyword in high_impact_keywords:
            if keyword in title:
                score += 0.2
                break

        if re.search(r'\d+億円|\d+百万円|\d+千万円', title):
            score += 0.1

        if len(disclosure.get('title', '')) > 50:
            score += 0.1

        return min(score, 1.0)

    def classify_disclosure_type(self, disclosure: Dict) -> str:
        """開示タイプの分類"""
        category = disclosure.get('category_detailed', '')
        title = disclosure.get('title', '').lower()

        if any(keyword in title for keyword in ['有価証券報告書', '四半期報告書', '決算短信']):
            return '法定開示'
        elif any(keyword in category for keyword in ['業績予想', 'M&A', '人事', '配当']):
            return '適時開示'
        elif any(keyword in title for keyword in ['説明資料', 'プレゼン', '説明会']):
            return 'IR資料'
        else:
            return 'その他'

    def create_stock_summary(self, stock_code: str, disclosures: List[Dict]) -> Dict:
        """銘柄サマリーの作成 - 開示データから情報を抽出"""
        if not disclosures:
            return {}

        total_disclosures = len(disclosures)
        date_range = self.get_date_range(disclosures)

        category_stats = Counter(d.get('category_detailed', 'その他') for d in disclosures)
        yearly_stats = Counter(d.get('year') for d in disclosures if d.get('year'))

        quarterly_stats = defaultdict(lambda: defaultdict(int))
        for d in disclosures:
            year = d.get('year')
            quarter = d.get('quarter')
            if year and quarter:
                quarterly_stats[year][f"Q{quarter}"] += 1

        importance_distribution = {
            'high': len([d for d in disclosures if d.get('importance_score', 0) >= 0.7]),
            'medium': len([d for d in disclosures if 0.4 <= d.get('importance_score', 0) < 0.7]),
            'low': len([d for d in disclosures if d.get('importance_score', 0) < 0.4])
        }

        company_names = [d.get('company_name_cleaned') or d.get('company_name', '')
                        for d in disclosures
                        if d.get('company_name_cleaned') or d.get('company_name', '')]
        company_names = [name for name in company_names if name and name != f"銘柄{stock_code}"]

        if company_names:
            name_counter = Counter(company_names)
            most_common_name = name_counter.most_common(1)[0][0]
        else:
            most_common_name = f"銘柄{stock_code}"

        company_info = {
            'stock_code': stock_code,
            'company_name': most_common_name,
            'company_name_variations': list(set(company_names[:5])),
            'data_source': '開示データから抽出'
        }

        major_disclosures = sorted(
            [d for d in disclosures if d.get('importance_score', 0) >= 0.6],
            key=lambda x: (x.get('date_normalized', ''), x.get('importance_score', 0)),
            reverse=True
        )[:20]

        # 期間を計算
        if date_range.get('start_date') and date_range.get('end_date'):
            try:
                start = datetime.strptime(date_range['start_date'], '%Y-%m-%d')
                end = datetime.strptime(date_range['end_date'], '%Y-%m-%d')
                period_years = (end - start).days / 365.25
            except:
                period_years = len(yearly_stats)
        else:
            period_years = len(yearly_stats)

        return {
            'company_info': company_info,
            'summary_stats': {
                'total_disclosures': total_disclosures,
                'date_range': date_range,
                'analysis_period_years': round(period_years, 1),
                'average_disclosures_per_year': round(total_disclosures / max(period_years, 1), 1)
            },
            'category_distribution': dict(category_stats.most_common()),
            'yearly_trend': dict(yearly_stats),
            'quarterly_trend': dict(quarterly_stats),
            'importance_distribution': importance_distribution,
            'major_disclosures': major_disclosures
        }

    def get_date_range(self, disclosures: List[Dict]) -> Dict[str, str]:
        """日付範囲の取得"""
        dates = [d.get('date_normalized') for d in disclosures if d.get('date_normalized')]
        dates = [d for d in dates if d and re.match(r'^\d{4}-\d{2}-\d{2}$', d)]

        if dates:
            return {
                'start_date': min(dates),
                'end_date': max(dates)
            }
        return {'start_date': '', 'end_date': ''}

    def save_stock_data_to_s3(self, stock_code: str, disclosures: List[Dict], is_update: bool = False) -> bool:
        """銘柄別データをS3に保存"""
        try:
            sorted_disclosures = sorted(disclosures, key=lambda x: x.get('date_normalized', ''))
            summary = self.create_stock_summary(stock_code, sorted_disclosures)

            stock_data = {
                'metadata': {
                    'stock_code': stock_code,
                    'created_at': datetime.now().isoformat(),
                    'data_version': '1.0',
                    'source': '株探5年分データ',
                    'last_updated': datetime.now().isoformat()
                },
                'summary': summary,
                'disclosures': sorted_disclosures
            }

            key = f"{self.output_prefix}{stock_code}.json"
            json_data = json.dumps(stock_data, ensure_ascii=False, indent=2)

            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json_data.encode('utf-8'),
                ContentType='application/json',
                ServerSideEncryption='AES256'
            )

            file_size_kb = len(json_data) / 1024
            action = "更新" if is_update else "作成"
            self.logger.info(f"{action}完了: {stock_code} ({len(sorted_disclosures)}件, {file_size_kb:.1f}KB)")
            
            if is_update:
                self.stats['updated_files'] += 1
            else:
                self.stats['created_files'] += 1

            return True

        except Exception as e:
            self.logger.error(f"保存エラー ({stock_code}): {e}")
            self.stats['errors'] += 1
            return False

    def create_master_index(self, stock_data: Dict[str, List[Dict]]):
        """銘柄インデックスファイルの作成"""
        try:
            index_data = {
                'metadata': {
                    'created_at': datetime.now().isoformat(),
                    'total_stocks': len(stock_data),
                    'total_disclosures': sum(len(disclosures) for disclosures in stock_data.values()),
                    'data_source': '株探5年分データ'
                },
                'stocks': {}
            }

            for stock_code, disclosures in stock_data.items():
                if disclosures:
                    latest = max(disclosures, key=lambda x: x.get('date_normalized', ''), default={})
                    date_range = self.get_date_range(disclosures)

                    index_data['stocks'][stock_code] = {
                        'company_name': latest.get('company_name_cleaned') or latest.get('company_name', '不明'),
                        'total_disclosures': len(disclosures),
                        'date_range': date_range,
                        'file_path': f"{self.output_prefix}{stock_code}.json"
                    }

            index_key = f"{self.output_prefix}index.json"
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=index_key,
                Body=json.dumps(index_data, ensure_ascii=False, indent=2).encode('utf-8'),
                ContentType='application/json'
            )

            self.logger.info(f"インデックスファイル作成完了: {index_key}")

        except Exception as e:
            self.logger.error(f"インデックスファイル作成エラー: {e}")

    def process_stocks_in_batches(self, new_stock_data: Dict[str, List[Dict]], batch_size: int = 50):
        """銘柄データをバッチ処理（モード別処理）"""
        stock_codes = list(new_stock_data.keys())
        total_stocks = len(stock_codes)

        self.logger.info(f"\n銘柄データ処理開始: {total_stocks:,}銘柄 (mode={self.mode})")

        for i, stock_code in enumerate(stock_codes, 1):
            try:
                if self.mode == 'incremental':
                    # 差分更新モード：既存データを読み込んでマージ
                    existing_data = self.load_existing_stock_data(stock_code)
                    
                    if existing_data and 'disclosures' in existing_data:
                        # 既存データあり：マージ
                        existing_disclosures = existing_data['disclosures']
                        new_disclosures = new_stock_data[stock_code]
                        merged_disclosures = self.merge_disclosures(existing_disclosures, new_disclosures)
                        
                        if len(merged_disclosures) > len(existing_disclosures):
                            # 新規データがあった場合のみ保存
                            self.logger.info(f"[{i}/{total_stocks}] 更新: {stock_code} (+{len(merged_disclosures)-len(existing_disclosures)}件)")
                            self.save_stock_data_to_s3(stock_code, merged_disclosures, is_update=True)
                        else:
                            # 新規データなし
                            self.logger.info(f"[{i}/{total_stocks}] スキップ: {stock_code} (変更なし)")
                    else:
                        # 既存データなし：新規作成
                        self.logger.info(f"[{i}/{total_stocks}] 新規作成: {stock_code}")
                        self.save_stock_data_to_s3(stock_code, new_stock_data[stock_code], is_update=False)
                
                else:
                    # フルモード：常に上書き
                    self.logger.info(f"[{i}/{total_stocks}] 処理: {stock_code}")
                    self.save_stock_data_to_s3(stock_code, new_stock_data[stock_code], is_update=False)

                # 進捗表示
                if i % 100 == 0:
                    elapsed = time.time() - self.stats['start_time']
                    progress = i / total_stocks * 100
                    self.logger.info(f"  進捗: {progress:.1f}% ({i}/{total_stocks}) - 経過時間: {elapsed/60:.1f}分")

                time.sleep(0.1)

            except Exception as e:
                self.logger.error(f"処理エラー ({stock_code}): {e}")
                self.stats['errors'] += 1

        self.logger.info("銘柄データ処理完了")

    def run_stock_based_organization(self):
        """銘柄別データ整理の実行"""
        print("=" * 80)
        print(f"株探データ銘柄別整理システム (mode={self.mode})")
        print("=" * 80)

        self.stats['start_time'] = time.time()

        try:
            # Step 1: データ読み込み
            self.logger.info("Step 1: データ読み込み開始")
            stock_data = self.load_all_monthly_data()

            if not stock_data:
                self.logger.error("データが読み込めませんでした")
                return False

            # Step 2: 統計表示
            if self.mode == 'full':
                self.logger.info("Step 2: データ統計")
                self.display_statistics(stock_data)

            # Step 3: 銘柄別ファイル作成/更新
            self.logger.info("Step 3: 銘柄別ファイル処理開始")
            self.process_stocks_in_batches(stock_data)

            # Step 4: インデックス作成（fullモード時のみ）
            if self.mode == 'full':
                self.logger.info("Step 4: マスターインデックス作成")
                self.create_master_index(stock_data)

            # Step 5: 完了レポート
            self.display_completion_report()

            return True

        except Exception as e:
            self.logger.error(f"実行エラー: {e}")
            import traceback
            traceback.print_exc()
            return False

    def display_statistics(self, stock_data: Dict[str, List[Dict]]):
        """統計情報の表示"""
        print("\n" + "=" * 60)
        print("データ統計")
        print("=" * 60)

        total_stocks = len(stock_data)
        total_disclosures = sum(len(disclosures) for disclosures in stock_data.values())

        print(f"対象銘柄数: {total_stocks:,}")
        print(f"総開示件数: {total_disclosures:,}")
        print(f"銘柄平均開示件数: {total_disclosures / max(total_stocks, 1):.1f}")

        # 上位銘柄（開示件数順）
        top_stocks = sorted(
            [(code, len(disclosures)) for code, disclosures in stock_data.items()],
            key=lambda x: x[1],
            reverse=True
        )[:10]

        print("\n【開示件数上位10銘柄】")
        for i, (stock_code, count) in enumerate(top_stocks, 1):
            company_name = "不明"
            if stock_data[stock_code]:
                latest = stock_data[stock_code][0]
                company_name = latest.get('company_name_cleaned') or latest.get('company_name', '不明')
            print(f"  {i:2d}. {stock_code} ({company_name}): {count:,}件")

    def display_completion_report(self):
        """完了レポートの表示"""
        elapsed_time = time.time() - self.stats['start_time']

        print("\n" + "=" * 80)
        print("銘柄別データ整理完了レポート")
        print("=" * 80)
        print(f"実行モード: {self.mode}")
        print(f"処理時間: {elapsed_time/60:.1f}分")
        print(f"処理月数: {self.stats['processed_months']}")
        print(f"対象銘柄数: {self.stats['unique_stocks']:,}")
        print(f"読込開示件数: {self.stats['total_disclosures']:,}")
        
        if self.mode == 'incremental':
            print(f"新規開示件数: {self.stats['new_disclosures']:,}")
            print(f"重複除外件数: {self.stats['duplicate_disclosures']:,}")
            print(f"更新ファイル数: {self.stats['updated_files']:,}")
            print(f"新規ファイル数: {self.stats['created_files']:,}")
        else:
            print(f"作成ファイル数: {self.stats['created_files']:,}")
        
        print(f"エラー件数: {self.stats['errors']}")

        if (self.stats['created_files'] + self.stats['updated_files']) > 0:
            total_files = self.stats['created_files'] + self.stats['updated_files']
            print(f"平均処理時間/銘柄: {elapsed_time/total_files:.2f}秒")

        print(f"\n保存先: s3://{self.bucket_name}/{self.output_prefix}")
        print("=" * 80)

    def get_sample_stock_data(self, stock_code: str = "1301") -> Dict:
        """サンプル銘柄データの取得（デバッグ用）"""
        try:
            key = f"{self.output_prefix}{stock_code}.json"
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            data = json.loads(response['Body'].read().decode('utf-8'))
            return data
        except Exception as e:
            self.logger.error(f"サンプルデータ取得エラー ({stock_code}): {e}")
            return {}


def main():
    """メイン実行関数（Colab対応版）"""
    # 環境変数またはデフォルトでモードを決定
    mode = os.getenv('STOCK_ORGANIZER_MODE', 'full')
    
    try:
        organizer = StockBasedDataOrganizer(mode=mode)
        success = organizer.run_stock_based_organization()

        if success:
            print(f"✅ 銘柄別データ整理が正常に完了しました (mode={mode})")

            # サンプル表示
            if mode == 'full':
                print(f"\nサンプル: 銘柄1301のデータ構造")
                sample = organizer.get_sample_stock_data("1301")
                if sample and 'summary' in sample:
                    summary = sample['summary']
                    print(f"  会社名: {summary.get('company_info', {}).get('company_name', '不明')}")
                    print(f"  開示件数: {summary.get('summary_stats', {}).get('total_disclosures', 0)}件")
                    print(f"  期間: {summary.get('summary_stats', {}).get('date_range', {})}")
        else:
            print("❌ 処理中にエラーが発生しました")

    except Exception as e:
        print(f"❌ 実行エラー: {e}")
        import traceback
        traceback.print_exc()


# Colab用の直接実行関数
def run_debug_path():
    """S3パス確認用関数（Colab直接実行用）"""
    organizer = StockBasedDataOrganizer()

    print("S3パス確認モード")
    print(f"バケット: {organizer.bucket_name}")
    print(f"取得パス: {organizer.source_prefix}")
    print(f"出力パス: {organizer.output_prefix}")

    try:
        paginator = organizer.s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=organizer.bucket_name, Prefix=organizer.source_prefix)

        print(f"\n{organizer.source_prefix} 内のファイル:")
        file_count = 0
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    print(f"  {obj['Key']}")
                    file_count += 1
                    if file_count >= 10:
                        break
            if file_count >= 10:
                break

        if file_count == 0:
            print("  ファイルが見つかりません")
        else:
            print(f"  総計: {file_count}+ 件のファイル")

    except Exception as e:
        print(f"S3アクセスエラー: {e}")


def run_full():
    """フルモード実行用（Colab直接実行用）"""
    os.environ['STOCK_ORGANIZER_MODE'] = 'full'
    organizer = StockBasedDataOrganizer(mode='full')
    success = organizer.run_stock_based_organization()
    
    if success:
        print("✅ フルモード処理が正常に完了しました")
    else:
        print("❌ 処理中にエラーが発生しました")


def run_incremental():
    """差分更新モード実行用（Colab直接実行用）"""
    os.environ['STOCK_ORGANIZER_MODE'] = 'incremental'
    organizer = StockBasedDataOrganizer(mode='incremental')
    success = organizer.run_stock_based_organization()
    
    if success:
        print("✅ 差分更新が正常に完了しました")
    else:
        print("❌ 処理中にエラーが発生しました")


def get_sample(stock_code="1301"):
    """サンプルデータ取得用関数（Colab直接実行用）"""
    organizer = StockBasedDataOrganizer()

    print(f"銘柄 {stock_code} のサンプルデータ:")
    sample_data = organizer.get_sample_stock_data(stock_code)
    if sample_data:
        print(json.dumps(sample_data, ensure_ascii=False, indent=2)[:2000] + "...")
    else:
        print("サンプルデータが見つかりません")


if __name__ == "__main__":
    main()
