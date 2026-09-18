from __future__ import annotations


def build_shadow_strategy_page(site_nav: str) -> str:
    return f"""
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="robots" content="noindex, nofollow, noarchive, nosnippet" />
  <title>Теневая стратегия Oil Bot</title>
  <link rel="icon" href="/favicon.ico" type="image/svg+xml" />
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Sora:wght@500;600;700&family=Manrope:wght@400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
  <style>
    :root {{ --bg:#030711; --surface:#081120; --surface2:#0b1628; --ink:#edf6ff; --muted:#8297b4; --line:rgba(102,174,255,.18); --good:#37e6a4; --bad:#ff6b87; --warn:#ffca62; --accent:#43c5ff; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; min-height:100vh; color:var(--ink); background:linear-gradient(180deg,#08111f 0%,var(--bg) 45%); font-family:"Manrope",sans-serif; }}
    .site-header {{ position:sticky; top:0; z-index:20; background:rgba(4,9,18,.94); backdrop-filter:blur(16px); border-bottom:1px solid var(--line); }}
    .site-header__inner,.wrap {{ width:min(1380px,100%); margin:0 auto; }}
    .site-header__inner {{ padding:18px 28px; display:flex; align-items:center; justify-content:space-between; gap:20px; }}
    .site-brand__eyebrow {{ margin-bottom:6px; color:#14f1ff; font:700 12px/1 "JetBrains Mono",monospace; letter-spacing:.16em; text-transform:uppercase; }}
    .site-brand__title {{ font:700 18px/1.1 "Sora",sans-serif; }}
    .site-nav {{ display:flex; flex-wrap:wrap; gap:10px; }}
    .site-nav__link {{ padding:10px 14px; color:#b8cae3; text-decoration:none; font-weight:600; border:1px solid var(--line); border-radius:8px; }}
    .site-nav__link.is-active {{ color:#fff; background:rgba(67,197,255,.16); border-color:rgba(67,197,255,.44); }}
    .wrap {{ padding:30px 28px 56px; }}
    h1,h2,h3 {{ margin:0; font-family:"Sora",sans-serif; letter-spacing:0; }}
    h1 {{ font-size:32px; }} h2 {{ font-size:22px; }} h3 {{ font-size:17px; }}
    .page-head {{ display:flex; align-items:flex-start; justify-content:space-between; gap:20px; padding-bottom:24px; border-bottom:1px solid var(--line); }}
    .page-head p {{ max-width:760px; margin:10px 0 0; color:var(--muted); line-height:1.55; }}
    .updated {{ color:var(--muted); font:500 12px/1.5 "JetBrains Mono",monospace; text-align:right; }}
    .status {{ display:inline-flex; margin-bottom:8px; padding:5px 8px; border:1px solid rgba(55,230,164,.3); border-radius:8px; color:var(--good); font-size:12px; font-weight:700; }}
    .section {{ padding:24px 0; border-bottom:1px solid var(--line); }}
    .section-head {{ display:flex; align-items:flex-start; justify-content:space-between; gap:16px; margin-bottom:16px; }}
    .muted {{ color:var(--muted); }} .good {{ color:var(--good); }} .bad {{ color:var(--bad); }} .warn {{ color:var(--warn); }} .mono {{ font-family:"JetBrains Mono",monospace; }}
    .comparison-grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:14px; }}
    .strategy-summary {{ min-width:0; padding:18px; border:1px solid var(--line); border-radius:8px; background:rgba(8,17,32,.78); }}
    .strategy-summary.shadow {{ border-color:rgba(55,230,164,.28); }}
    .strategy-label {{ color:var(--muted); font-size:12px; text-transform:uppercase; }}
    .strategy-title {{ margin-top:6px; font:700 19px/1.3 "Sora",sans-serif; }}
    .metric-grid {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:9px; margin-top:16px; }}
    .metric {{ min-width:0; min-height:88px; padding:11px; border:1px solid rgba(102,174,255,.12); border-radius:8px; background:rgba(3,8,17,.58); }}
    .metric-label {{ color:var(--muted); font-size:11px; }}
    .metric-value {{ margin-top:7px; font:700 20px/1.15 "Sora",sans-serif; overflow-wrap:anywhere; }}
    .metric-note {{ margin-top:5px; color:var(--muted); font-size:11px; line-height:1.35; }}
    .findings {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:10px; margin-top:14px; }}
    .finding {{ padding:13px 14px; border-left:2px solid var(--accent); background:rgba(67,197,255,.05); line-height:1.45; }}
    .finding strong {{ display:block; margin-bottom:4px; }}
    .horizon-grid {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:10px; margin-bottom:14px; }}
    .horizon {{ min-width:0; padding:14px; border:1px solid var(--line); border-radius:8px; background:rgba(8,17,32,.64); }}
    .horizon-title {{ font-weight:700; }} .horizon-value {{ margin-top:9px; font:700 18px/1.2 "JetBrains Mono",monospace; }}
    .horizon-note {{ margin-top:6px; color:var(--muted); font-size:11px; line-height:1.4; }}
    .readiness {{ margin:0 0 16px; padding:18px; border:1px solid rgba(67,197,255,.34); border-radius:10px; background:linear-gradient(135deg,rgba(67,197,255,.10),rgba(8,17,32,.72)); }}
    .readiness-head {{ display:flex; align-items:flex-start; justify-content:space-between; gap:16px; }}
    .readiness-status {{ flex:0 0 auto; padding:6px 9px; border:1px solid var(--line); border-radius:8px; font-size:11px; font-weight:700; }}
    .readiness-status.wait {{ color:var(--warn); border-color:rgba(255,202,98,.4); }} .readiness-status.ready {{ color:var(--good); border-color:rgba(55,230,164,.4); }} .readiness-status.stop {{ color:var(--bad); border-color:rgba(255,107,135,.4); }}
    .readiness-text {{ margin:8px 0 0; color:#cfdded; line-height:1.5; }}
    .tabs {{ display:flex; gap:8px; overflow:auto; padding:20px 0 0; scrollbar-width:thin; }}
    .tab {{ flex:0 0 auto; padding:9px 12px; border:1px solid var(--line); border-radius:8px; background:transparent; color:var(--muted); font:700 13px/1 "Manrope",sans-serif; cursor:pointer; }}
    .tab.active {{ color:#fff; border-color:rgba(67,197,255,.5); background:rgba(67,197,255,.16); }}
    .tab-panel {{ display:none; padding-top:18px; }} .tab-panel.active {{ display:block; }}
    .table-scroll {{ overflow:auto; border:1px solid rgba(102,174,255,.12); border-radius:8px; }}
    table {{ width:100%; min-width:920px; border-collapse:collapse; font-size:13px; }}
    th,td {{ padding:12px; text-align:left; border-bottom:1px solid rgba(102,174,255,.12); vertical-align:top; }}
    th {{ color:#a9bed8; font-size:11px; text-transform:uppercase; }} tr:last-child td {{ border-bottom:0; }}
    .instrument-name {{ margin-top:3px; color:var(--muted); font-size:11px; max-width:240px; }}
    .events {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:10px; }}
    .event {{ min-width:0; padding:15px; border:1px solid rgba(102,174,255,.14); border-radius:8px; background:rgba(8,17,32,.64); }}
    .event-head {{ display:flex; align-items:flex-start; justify-content:space-between; gap:12px; }}
    .event-title {{ font-weight:700; }} .event-time {{ margin-top:3px; color:var(--muted); font-size:11px; }}
    .badge {{ flex:0 0 auto; padding:4px 7px; border:1px solid var(--line); border-radius:8px; font-size:11px; font-weight:700; }}
    .badge.entry {{ color:var(--good); border-color:rgba(55,230,164,.28); }} .badge.exit {{ color:var(--warn); border-color:rgba(255,202,98,.28); }}
    .event-result {{ margin:12px 0 8px; font:700 18px/1.2 "JetBrains Mono",monospace; }}
    .event-details {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:8px; margin-top:11px; }}
    .event-detail {{ padding:8px; background:rgba(3,8,17,.5); border-radius:6px; }}
    .event-detail span {{ display:block; color:var(--muted); font-size:10px; }} .event-detail b {{ display:block; margin-top:3px; font-size:12px; overflow-wrap:anywhere; }}
    .event-reason {{ margin-top:11px; color:#cfdded; font-size:12px; line-height:1.5; }}
    .empty,.error {{ padding:18px; color:var(--muted); border:1px dashed var(--line); border-radius:8px; }} .error {{ color:var(--bad); }}
    @media (max-width:900px) {{ .comparison-grid,.events {{ grid-template-columns:1fr; }} .findings {{ grid-template-columns:1fr; }} .horizon-grid {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} }}
    @media (max-width:640px) {{ .site-header__inner,.page-head,.section-head {{ flex-direction:column; }} .site-header__inner,.wrap {{ padding-left:16px; padding-right:16px; }} .updated {{ text-align:left; }} h1 {{ font-size:27px; }} .metric-grid {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} .event-details {{ grid-template-columns:1fr; }} }}
  </style>
</head>
<body>
  {site_nav}
  <main class="wrap">
    <section class="page-head">
      <div>
        <div class="status" id="strategyStatus">ЗАГРУЗКА</div>
        <h1>Теневая стратегия</h1>
        <p>Вход после двух закрытых усиливающихся свечей AO, выход после подтверждённого истощения импульса. Поток объёма Чайкина влияет только на оценку качества. Реальные заявки не меняются.</p>
      </div>
      <div class="updated" id="updatedAt">Обновление: -</div>
    </section>

    <section class="section" id="strategyComparison">
      <div class="section-head"><div><h2>Сравнение стратегий</h2><div class="muted" id="comparisonPeriod">Период закрытий ещё рассчитывается.</div></div><div class="muted">Один лот после комиссии</div></div>
      <div class="comparison-grid">
        <article class="strategy-summary"><div class="strategy-label">Рабочая стратегия</div><div class="strategy-title">Часовой разворот</div><div class="metric-grid" id="currentMetrics"></div></article>
        <article class="strategy-summary shadow"><div class="strategy-label">Теневой эксперимент</div><div class="strategy-title">AO и поток Чайкина</div><div class="metric-grid" id="shadowMetrics"></div></article>
      </div>
      <div class="findings" id="comparisonFindings"></div>
    </section>

    <div class="tabs" role="tablist" aria-label="Разделы теневой стратегии">
      <button class="tab active" type="button" data-tab="instruments">Инструменты</button>
      <button class="tab" type="button" data-tab="exits">Удержание и выходы</button>
      <button class="tab" type="button" data-tab="positions">Открытые позиции</button>
      <button class="tab" type="button" data-tab="trades">Закрытые сделки</button>
      <button class="tab" type="button" data-tab="decisions">Журнал решений</button>
    </div>

    <section class="tab-panel active" id="tabInstruments"><div class="section-head"><div><h2>По инструментам</h2><div class="muted">Где новая схема улучшает или ухудшает результат.</div></div></div><div class="table-scroll"><table><thead><tr><th>Инструмент</th><th>Рабочая: сделки</th><th>Рабочая: в плюс</th><th>Рабочая: итог</th><th>Теневая: сделки</th><th>Теневая: в плюс</th><th>Теневая: итог</th><th>Разница</th></tr></thead><tbody id="instrumentComparison"></tbody></table></div></section>
    <section class="tab-panel" id="tabExits"><div class="section-head"><div><h2>Удержание после выхода</h2><div class="muted" id="exitAnalyticsSummary">Проверяем следующие закрытые часовые свечи.</div></div><div class="muted">Один лот</div></div><div class="readiness" id="conditionalReadiness"></div><div class="horizon-grid" id="exitHorizons"></div><div class="findings" id="exitFindings"></div><div class="table-scroll" style="margin-top:14px"><table><thead><tr><th>Сделка</th><th>Фактический выход</th><th>Ещё 1 свеча</th><th>Ещё 2 свечи</th><th>Ещё 4 свечи</th><th>Ещё 8 свечей</th><th>Удержано от максимума</th></tr></thead><tbody id="exitTrades"></tbody></table></div></section>
    <section class="tab-panel" id="tabPositions"><div class="section-head"><div><h2>Открытые позиции</h2><div class="muted">Текущая оценка на один лот.</div></div></div><div class="events" id="shadowStrategyOpen"></div></section>
    <section class="tab-panel" id="tabTrades"><div class="section-head"><div><h2>Закрытые сделки</h2><div class="muted">Сначала самые свежие результаты.</div></div></div><div class="events" id="shadowStrategyTrades"></div></section>
    <section class="tab-panel" id="tabDecisions"><div class="section-head"><div><h2>Журнал решений</h2><div class="muted">Входы, удержания, выходы и причины.</div></div></div><div class="events" id="shadowStrategyDecisions"></div></section>
  </main>
  <script>
    const esc = (value) => String(value ?? '').replace(/[&<>'"]/g, (char) => ({{'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}}[char]));
    const signedRub = (value) => {{ const number=Number(value||0); return `${{number>0?'+':''}}${{number.toLocaleString('ru-RU',{{minimumFractionDigits:2,maximumFractionDigits:2}})}} RUB`; }};
    const pct = (value) => value == null ? '-' : `${{Number(value).toFixed(1)}}%`;
    const tone = (value) => Number(value||0)>0?'good':Number(value||0)<0?'bad':'';
    const time = (value) => {{ if(!value)return '-'; const date=new Date(value); return Number.isNaN(date.getTime())?String(value):date.toLocaleString('ru-RU',{{timeZone:'Europe/Moscow',day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'}}); }};
    const metric = (label,value,note='',cls='') => `<div class="metric"><div class="metric-label">${{esc(label)}}</div><div class="metric-value ${{cls}}">${{esc(value)}}</div><div class="metric-note">${{esc(note)}}</div></div>`;
    const instrument = (symbol,catalog) => catalog?.[symbol] || symbol || '-';

    function renderSummary(target, data) {{
      document.getElementById(target).innerHTML = [
        metric('Закрыто',String(data.closed_trades||0),`${{data.wins||0}} в плюс · ${{data.losses||0}} в минус`),
        metric('В плюс',`${{Number(data.wins||0)}} из ${{Number(data.closed_trades||0)}}`,pct(data.win_rate_pct)+' закрытых сделок'),
        metric('Итог на 1 лот',signedRub(data.net_result_rub_1lot),'после комиссии',tone(data.net_result_rub_1lot)),
        metric('До комиссии',signedRub(data.gross_result_rub_1lot),'движение цены',tone(data.gross_result_rub_1lot)),
        metric('Комиссии',signedRub(-Number(data.commission_rub_1lot||0)),'оценка расходов','bad'),
        metric('Удержали движение',pct(data.average_capture_pct),'среднее по сделкам'),
      ].join('');
    }}

    function eventCard(item,catalog) {{
      const result=item.estimated_net_rub_1lot;
      const isExit=String(item.decision||'').includes('ВЫХОД');
      const rollover=item.exit_kind==='СМЕНА КОНТРАКТА' ? `<div class="event-reason warn"><strong>Смена контракта:</strong> продолжение в ${{esc(item.rollover_to_symbol||'-')}}.</div>` : '';
      return `<article class="event"><div class="event-head"><div><div class="event-title">${{esc(instrument(item.symbol,catalog))}}</div><div class="event-time">${{esc(time(item.candle_closed_at))}} · ${{esc(String(item.direction||'нет').toLowerCase())}}</div></div><span class="badge ${{isExit?'exit':'entry'}}">${{esc(item.decision||'НЕТ ВХОДА')}}</span></div>
        ${{result==null?'':`<div class="event-result ${{tone(result)}}">${{esc(signedRub(result))}}</div>`}}
        <div class="event-details"><div class="event-detail"><span>Сила AO относительно обычного движения</span><b>${{Number(item.ao_strength_atr_ratio||0).toFixed(2)}} раза</b></div><div class="event-detail"><span>Остаток импульса от пика</span><b>${{item.ao_peak_retention_ratio==null?'-':`${{(Number(item.ao_peak_retention_ratio)*100).toFixed(1)}}%`}}</b></div><div class="event-detail"><span>Ослаблений AO подряд</span><b>${{Number(item.opposite_ao_bars||0)}} из 3</b></div><div class="event-detail"><span>Цена подтвердила выход</span><b>${{item.price_confirms_exit?'да':'нет'}}</b></div><div class="event-detail"><span>Поток объёма Чайкина</span><b>${{esc(String(item.chaikin_status||'нейтрален').toLowerCase())}}</b></div><div class="event-detail"><span>Цена</span><b class="mono">${{Number(item.price||0).toLocaleString('ru-RU')}}</b></div></div>
        <div class="event-reason">${{esc(item.reason||'Причина не сохранена')}}</div>${{rollover}}</article>`;
    }}

    function renderEvents(target,items,catalog,limit,empty) {{
      document.getElementById(target).innerHTML = items.length ? items.slice(0,limit).map((item)=>eventCard(item,catalog)).join('') : `<div class="empty">${{esc(empty)}}</div>`;
    }}

    function renderConditionalReadiness(conditional) {{
      const readiness=conditional?.readiness||{{}};
      const evaluated=Number(conditional?.evaluated||0); const target=Number(readiness.target_evaluated||20);
      const status=readiness.status||'data_insufficient';
      const view=status==='ready_for_limited_trial'
        ? {{label:'МОЖНО ГОТОВИТЬ ОГРАНИЧЕННУЮ ПРОВЕРКУ',cls:'ready',text:'Условия наблюдения выполнены. Это не меняет реальные выходы: отдельное решение принимается только после повторной проверки результатов.'}}
        : status==='not_confirmed'
        ? {{label:'ГИПОТЕЗА ПОКА НЕ ПОДТВЕРЖДЕНА',cls:'stop',text:'Набран достаточный объём, но хотя бы один критерий качества не выполнен. Реальные выходы остаются без изменений.'}}
        : {{label:'ДАННЫХ ПОКА МАЛО',cls:'wait',text:`Нужно ещё ${{Math.max(0,Number(readiness.remaining ?? target-evaluated))}} завершённых наблюдений. Реальные выходы не меняются.`}};
      const dominant=readiness.dominant_symbol ? `${{readiness.dominant_symbol}} · ${{pct(readiness.dominant_symbol_share_pct)}} вклада` : 'пока нет';
      document.getElementById('conditionalReadiness').innerHTML=`<div class="readiness-head"><div><h3>Готовность проверки: удержать ещё 2 часа</h3><div class="readiness-text">Подходят только выходы при истощении импульса, когда сохранено не менее половины пика и поток Чайкина не подтверждает разворот.</div></div><div class="readiness-status ${{view.cls}}">${{view.label}}</div></div><div class="metric-grid"><div class="metric"><div class="metric-label">Завершено наблюдений</div><div class="metric-value">${{evaluated}} из ${{target}}</div><div class="metric-note">минимум для вывода</div></div><div class="metric"><div class="metric-label">Улучшение результата</div><div class="metric-value ${{tone(conditional?.delta_rub_1lot)}}">${{esc(signedRub(conditional?.delta_rub_1lot))}}</div><div class="metric-note">после комиссии, один лот</div></div><div class="metric"><div class="metric-label">Когда было лучше</div><div class="metric-value">${{pct(conditional?.better_pct)}}</div><div class="metric-note">нужно не менее ${{Number(readiness.minimum_better_pct||60)}}%</div></div><div class="metric"><div class="metric-label">Разнообразие инструментов</div><div class="metric-value">${{Number(readiness.unique_symbols||0)}} из ${{Number(readiness.minimum_symbols||3)}}</div><div class="metric-note">лидер: ${{esc(dominant)}}</div></div></div><div class="readiness-text"><strong>Текущий вывод:</strong> ${{view.text}}</div>`;
    }}

    function renderExitAnalytics(data,catalog) {{
      const conditional=data?.conditional_two_hour_experiment||{{}};
      renderConditionalReadiness(conditional);
      if(!data?.available) {{
        document.getElementById('exitHorizons').innerHTML='<div class="empty">Для проверки выходов пока недостаточно закрытых сделок.</div>';
        document.getElementById('exitTrades').innerHTML='<tr><td colspan="7" class="muted">История ещё собирается.</td></tr>';
        return;
      }}
      document.getElementById('exitAnalyticsSummary').textContent=`Проверено ${{Number(data.closed_trades||0)}} выходов · в среднем удержано ${{pct(data.average_capture_pct)}} движения`;
      const horizons=Array.isArray(data.horizons)?data.horizons:[];
      document.getElementById('exitHorizons').innerHTML=horizons.map((row)=>`<div class="horizon"><div class="horizon-title">Ещё ${{row.additional_hours}} час. свеч.</div><div class="horizon-value ${{tone(row.delta_rub_1lot)}}">${{esc(signedRub(row.delta_rub_1lot))}}</div><div class="horizon-note">лучше в ${{pct(row.better_pct)}} случаев · проверено ${{row.evaluated}}</div></div>`).join('');
      const best=data.best_horizon;
      document.getElementById('exitFindings').innerHTML=[
        `<div class="finding"><strong>Вернули прибыль рынку</strong>${{Number(data.losses_after_profitable_move||0)}} сделок успевали покрыть комиссию, но закрылись в минус.</div>`,
        best?`<div class="finding"><strong>Лучший общий результат</strong>Ещё ${{best.additional_hours}} час. свеч.: ${{esc(signedRub(best.delta_rub_1lot))}} к фактическим выходам.</div>`:'<div class="finding"><strong>Простое ожидание не помогает</strong>Ни одно фиксированное окно пока не улучшило общий результат.</div>',
        best?`<div class="finding"><strong>Важное ограничение</strong>Такой выход был лучше только в ${{pct(best.better_pct)}} случаев. Нельзя просто задерживать каждую сделку.</div>`:'',
        conditional.evaluated?`<div class="finding"><strong>Условные 2 часа</strong>${{esc(conditional.rule||'')}}: лучше в ${{pct(conditional.better_pct)}} случаев, разница ${{esc(signedRub(conditional.delta_rub_1lot))}}. Это наблюдение, реальные выходы не меняются.</div>`:'<div class="finding"><strong>Условные 2 часа</strong>Наблюдение начато: ждём первых завершённых подходящих выходов.</div>',
      ].join('');
      const trades=Array.isArray(data.trades)?data.trades:[];
      const holdCell=(item,hours)=>{{const value=item.holds?.[String(hours)];return value?`<span class="mono ${{tone(value.delta_rub_1lot)}}">${{esc(signedRub(value.delta_rub_1lot))}}</span>`:'<span class="muted">ещё нет</span>';}};
      document.getElementById('exitTrades').innerHTML=trades.slice(0,24).map((item)=>`<tr><td><strong>${{esc(item.symbol)}}</strong><div class="instrument-name">${{esc(instrument(item.symbol,catalog))}} · ${{esc(String(item.direction||'').toLowerCase())}}</div><div class="instrument-name">${{esc(time(item.exit_time))}}</div></td><td class="mono ${{tone(item.actual_net_rub_1lot)}}">${{esc(signedRub(item.actual_net_rub_1lot))}}</td><td>${{holdCell(item,1)}}</td><td>${{holdCell(item,2)}}</td><td>${{holdCell(item,4)}}</td><td>${{holdCell(item,8)}}</td><td>${{pct(item.capture_pct)}}</td></tr>`).join('')||'<tr><td colspan="7" class="muted">Нет подходящих выходов.</td></tr>';
    }}

    async function load() {{
      const response=await fetch('/api/shadow-strategy',{{cache:'no-store'}});
      if(!response.ok)throw new Error('Не удалось загрузить теневую стратегию');
      const data=await response.json(); const shadow=data.strategy||{{}}; const comparison=data.comparison||{{}}; const exitAnalytics=data.exit_analytics||{{}}; const catalog=data.instrument_catalog||{{}};
      document.getElementById('strategyStatus').textContent=shadow.enabled?'НАБЛЮДЕНИЕ АКТИВНО':'НАБЛЮДЕНИЕ ВЫКЛЮЧЕНО';
      document.getElementById('updatedAt').textContent=`Обновление: ${{data.generated_at_moscow||'-'}}`;
      if(!comparison.available) {{ document.getElementById('strategyComparison').innerHTML='<div class="empty">Для сравнения пока недостаточно завершённых часовых наблюдений.</div>'; return; }}
      document.getElementById('comparisonPeriod').textContent=`${{time(comparison.period_start)}} — ${{time(comparison.period_end)}} · закрытия в период`;
      renderSummary('currentMetrics',comparison.current||{{}}); renderSummary('shadowMetrics',comparison.shadow||{{}});
      const diff=comparison.difference||{{}}; const exits=comparison.exit_diagnostics||{{}};
      document.getElementById('comparisonFindings').innerHTML=[
        `<div class="finding"><strong>Точность входов</strong><span class="${{tone(diff.win_rate_pct_points)}}">${{Number(diff.win_rate_pct_points||0)>0?'+':''}}${{Number(diff.win_rate_pct_points||0).toFixed(1)}} п.п.</span> у теневой схемы</div>`,
        `<div class="finding"><strong>Денежный результат</strong><span class="${{tone(diff.net_result_rub_1lot)}}">${{esc(signedRub(diff.net_result_rub_1lot))}}</span> к рабочей стратегии</div>`,
        `<div class="finding"><strong>Возврат прибыли</strong>${{Number(exits.losses_after_profitable_move||0)}} из ${{Number(exits.losses_total||0)}} убыточных сделок ранее покрывали комиссию</div>`,
      ].join('');
      const rows=Array.isArray(comparison.by_symbol)?comparison.by_symbol:[];
      document.getElementById('instrumentComparison').innerHTML=rows.map((row)=>{{const current=row.current||{{}};const experiment=row.shadow||{{}};const delta=Number(experiment.net_result_rub_1lot||0)-Number(current.net_result_rub_1lot||0);const positive=(metrics)=>`${{Number(metrics.wins||0)}} из ${{Number(metrics.closed_trades||0)}} (${{pct(metrics.win_rate_pct)}})`;return `<tr><td><strong>${{esc(row.symbol)}}</strong><div class="instrument-name">${{esc(instrument(row.symbol,catalog))}}</div></td><td>${{current.closed_trades||0}}</td><td>${{esc(positive(current))}}</td><td class="mono ${{tone(current.net_result_rub_1lot)}}">${{esc(signedRub(current.net_result_rub_1lot))}}</td><td>${{experiment.closed_trades||0}}</td><td>${{esc(positive(experiment))}}</td><td class="mono ${{tone(experiment.net_result_rub_1lot)}}">${{esc(signedRub(experiment.net_result_rub_1lot))}}</td><td class="mono ${{tone(delta)}}">${{esc(signedRub(delta))}}</td></tr>`;}}).join('')||'<tr><td colspan="8" class="muted">Нет данных по инструментам.</td></tr>';
      renderExitAnalytics(exitAnalytics,catalog);
      const open=Array.isArray(shadow.open_positions)?shadow.open_positions:[]; const trades=Array.isArray(shadow.closed_trades)?shadow.closed_trades:[]; const decisions=Array.isArray(shadow.decisions)?shadow.decisions:[];
      renderEvents('shadowStrategyOpen',open,catalog,30,'Открытых теневых позиций сейчас нет.');
      renderEvents('shadowStrategyTrades',trades,catalog,60,'Закрытых теневых сделок пока нет.');
      renderEvents('shadowStrategyDecisions',decisions,catalog,100,'Решения появятся после закрытия часовой свечи.');
    }}
    document.querySelectorAll('.tab').forEach((button)=>button.addEventListener('click',()=>{{const key=button.dataset.tab;document.querySelectorAll('.tab').forEach((item)=>item.classList.toggle('active',item===button));document.querySelectorAll('.tab-panel').forEach((panel)=>panel.classList.toggle('active',panel.id===`tab${{key.charAt(0).toUpperCase()}}${{key.slice(1)}}`));}}));
    const refresh=()=>load().catch((error)=>{{document.getElementById('strategyComparison').innerHTML=`<div class="error">${{esc(error.message)}}</div>`;}}); refresh(); setInterval(refresh,60000);
  </script>
</body>
</html>
"""
