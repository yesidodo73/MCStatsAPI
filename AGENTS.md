# MCStats 프로젝트 가이드

## 프로젝트 목표
이 프로젝트는 마인크래프트 서버의 플레이어 통계와 서버 텔레메트리를 수집/저장/조회하는 API 플랫폼을 만드는 것이 목표입니다.

핵심 목표:
- 대형 서버에서도 빠르고 효율적으로 동작
- 높은 안정성과 데이터 정합성 유지
- 원본 데이터 불변성 보장(원본 수정/삭제 금지)
- 기간별 통계 제공(`last1h`, `last24h`, `last7d`, `last30d`, `total`)
- UUID 차단 기능 제공
- 웹/디스코드 봇/인게임 플러그인 연동 지원

## 저장소 구조
- `API/`: ASP.NET Core 기반 API + 대시보드
- `PLUGIN/`: Paper 플러그인(비동기 배치 수집/전송)
- `AGENTS.md`: 루트 프로젝트 가이드

## API 설계 원칙
1. 원본 데이터 불변성
- `raw_events`는 append-only
- DB 트리거로 UPDATE/DELETE 차단

2. 기간별 집계 저장
- `agg_minutely`, `agg_hourly`, `agg_daily`, `agg_total`
- 조회 성능을 위해 집계 테이블 우선 사용

3. 용량 최적화
- metric 문자열은 `metric_catalog`로 정규화(정수 ID 사용)
- 불필요한 고카디널리티 데이터 저장 지양

4. 운영 제어
- `mcstats.config.json`에서 차단 UUID 관리

## 텔레메트리 원칙
현재 수집 항목:
- TPS, MSPT
- CPU 사용률
- RAM 사용량/총량
- 온라인 플레이어 수
- 핑 분포(p50/p95/p99)

확장 권장 항목:
- 네트워크 Rx/Tx 대역폭
- 디스크 I/O
- GC/스레드 지표

## 플러그인 연동 원칙
- 비동기 배치 전송
- 실패 시 재시도 큐 유지
- 필요 시 HMAC 서명 인증 사용
- `server-id` 기반으로 서버 단위 분리 수집

## 빌드 규칙
- API: `API/`에서 `dotnet build`
- Plugin: `PLUGIN/`에서 `./gradlew build`
- 변경 후 빌드 성공 여부 반드시 확인

## 실행 메모
- API 실행 위치: `API/`
- Plugin 빌드 위치: `PLUGIN/`
- 대시보드 경로: API 실행 후 `/`

## 인코딩 규칙 (중요)
- 모든 문서/텍스트 파일은 UTF-8로 유지한다.
- 한글 문서 수정 시 PowerShell 인코딩 변환 명령으로 저장하지 않는다.
- 텍스트 변경은 파일 직접 패치(에디터/패치) 방식으로 수행한다.
- 한글이 깨지면 파일을 다시 열기 전에 UTF-8 인코딩 상태를 먼저 확인한다.
- 매번 git commit / git push 를 수행한다.