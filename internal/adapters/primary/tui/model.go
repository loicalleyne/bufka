package tui

import (
	"context"
	"fmt"
	"time"

	"charm.land/bubbles/v2/spinner"
	tea "charm.land/bubbletea/v2"
)

// page identifies which top-level view is active.
type page int

const (
	pageSplash page = iota
	pageMenu
	pageList
	pageDetail
	pageGroups
	pageGroupDetail
	pageKafkaCache
	pageCacheAdd
	pageCacheDetail
	pageKafkaSettings
	pageProtoMessages
	pageProtoSettings
	pageTopicMapping
	pageProtoSchema
	pageAsyncDetail
	pageSchemaPicker
	pageDuckDBUISettings
	pageSQLRepl
)

// Model is the top-level Bubble Tea model, routing between the main menu,
// topic list, topic detail, consumer-groups, and schema views.
type Model struct {
	deps Deps
	ctx  context.Context

	page   page
	width  int
	height int

	status string
	err    error

	// The splash is left only once both the minimum display time has
	// elapsed and the initial topic list has loaded.
	splashElapsed bool
	topicsReady   bool

	splash        splashModel
	menu          menuModel
	list          topicListModel
	detail        detailModel
	groups        groupsModel
	groupDetail   groupDetailModel
	cacheList     cacheListModel
	cacheAdd      cacheAddModel
	cacheDetail   cacheDetailModel
	kafkaSettings kafkaSettingsModel
	protoMessages protoMessagesModel
	protoSettings protoSettingsModel
	duckDBUI      duckDBUISettingsModel
	topicMapping  topicMappingModel
	// detailBack is the page esc returns to from the topic detail screen.
	detailBack   page
	protoSchema  protoSchemaModel
	asyncDetail  asyncDetailModel
	schemaPicker schemaPickerModel
	sqlRepl      sqlReplModel
	// sqlReplOpen is true while sqlRepl holds an open session.
	sqlReplOpen bool
	// keyboardEnhanced: the terminal disambiguates shift+enter / ctrl+enter.
	keyboardEnhanced bool
	// pickerReturn is the page the schema picker returns to on esc.
	pickerReturn page
}

// New builds the top-level model. ctx bounds every Kafka/DuckDB call issued
// while the program runs; it should be canceled when the program exits.
func New(ctx context.Context, deps Deps) Model {
	m := Model{
		deps:          deps,
		ctx:           ctx,
		page:          pageSplash,
		splash:        newSplashModel(),
		menu:          newMenuModel(),
		list:          newTopicListModel(deps),
		cacheList:     newCacheListModel(deps),
		cacheAdd:      newCacheAddModel(deps),
		kafkaSettings: newKafkaSettingsModel(ctx, deps),
		protoMessages: newProtoMessagesModel(deps),
		protoSettings: newProtoSettingsModel(ctx, deps),
		duckDBUI:      newDuckDBUISettingsModel(ctx, deps),
	}
	m.menu.duckDBUISummary = m.duckDBUI.menuSummary()
	return m
}

type menuTickMsg time.Time

func menuAutoRefreshCmd() tea.Cmd {
	return tea.Tick(5*time.Second, func(t time.Time) tea.Msg {
		return menuTickMsg(t)
	})
}

func (m Model) Init() tea.Cmd {
	return tea.Batch(m.splash.Init(), m.list.Init(), splashTimerCmd(), menuAutoRefreshCmd())
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		var cmds []tea.Cmd
		m.splash = m.splash.resize(msg.Width, msg.Height-2)
		m.menu = m.menu.resize(msg.Width, msg.Height-2)
		m.list, _ = m.list.resize(msg.Width, msg.Height-2)
		var cmd tea.Cmd
		m.detail, cmd = m.detail.resize(msg.Width, msg.Height-3)
		cmds = append(cmds, cmd)
		m.groups, _ = m.groups.resize(msg.Width, msg.Height-2)
		m.groupDetail, _ = m.groupDetail.resize(msg.Width, msg.Height-2)
		m.cacheList, _ = m.cacheList.resize(msg.Width, msg.Height-2)
		m.cacheAdd = m.cacheAdd.resize(msg.Width, msg.Height-2)
		m.cacheDetail = m.cacheDetail.resize(msg.Width, msg.Height-2)
		m.kafkaSettings, _ = m.kafkaSettings.resize(msg.Width, msg.Height-2)
		m.protoMessages, _ = m.protoMessages.resize(msg.Width, msg.Height-2)
		m.protoSettings, _ = m.protoSettings.resize(msg.Width, msg.Height-2)
		m.topicMapping = m.topicMapping.resize(msg.Width, msg.Height-2)
		m.protoSchema, _ = m.protoSchema.resize(msg.Width, msg.Height-2)
		m.asyncDetail = m.asyncDetail.resize(msg.Width, msg.Height-2)
		m.schemaPicker = m.schemaPicker.resize(msg.Width, msg.Height-2)
		if m.sqlReplOpen {
			m.sqlRepl = m.sqlRepl.resize(msg.Width, msg.Height-2)
		}
		return m, tea.Batch(cmds...)

	case schemaOverriddenMsg:
		if msg.err != nil {
			m.err = msg.err
			return m, nil
		}
		m.schemaPicker, _ = m.schemaPicker.update(msg)
		// Rebuild views derived from kafka_topic_schemas / the _sample table.
		m.protoMessages = newProtoMessagesModel(m.deps)
		m.protoMessages, _ = m.protoMessages.resize(m.width, m.height-2)
		var cmds []tea.Cmd
		if m.detail.topic == msg.topic {
			m.detail.summaryOK = false
			m.detail.loading[tabSample] = true
			cmds = append(cmds, m.detail.loadSampleCmd())
		}
		m.list.loading = true
		cmds = append(cmds, m.list.Init(), setStatus(fmt.Sprintf("%s now decoded as %s (override saved)", msg.topic, msg.message)))
		return m, tea.Batch(cmds...)

	case tea.KeyPressMsg:
		if msg.String() == "ctrl+c" && m.page != pageSQLRepl {
			return m, tea.Quit
		}

	case statusMsg:
		m.status = string(msg)
		m.err = nil
		return m, nil

	case errMsg:
		m.err = msg.err
		m.status = ""
		return m, nil

	case menuSelectedMsg:
		switch msg.target {
		case menuTargetTopics:
			m.page = pageList
		case menuTargetGroups:
			m.page = pageGroups
			m.groups = newGroupsModel(m.deps)
			m.groups, _ = m.groups.resize(m.width, m.height-2)
			return m, m.groups.Init()
		case menuTargetKafkaCache:
			m.page = pageKafkaCache
			m.cacheList = newCacheListModel(m.deps)
			m.cacheList, _ = m.cacheList.resize(m.width, m.height-2)
			return m, m.cacheList.Init()
		case menuTargetKafkaSettings:
			m.page = pageKafkaSettings
		case menuTargetSchemaCatalog:
			m.page = pageProtoMessages
		case menuTargetTopicMapping:
			m.page = pageTopicMapping
			m.topicMapping = newTopicMappingModel(m.deps).resize(m.width, m.height-2)
			return m, m.topicMapping.Init()
		case menuTargetSchemaSources:
			m.page = pageProtoSettings
		case menuTargetDuckDBUI:
			m.page = pageDuckDBUISettings
		case menuTargetSQLRepl:
			m.page = pageSQLRepl
			if !m.sqlReplOpen {
				m.sqlRepl = newSQLReplModel(m.ctx, m.deps).resize(m.width, m.height-2)
				m.sqlRepl.enhanced = m.keyboardEnhanced
				m.sqlReplOpen = true
				return m, m.sqlRepl.Init()
			}
		}
		return m, nil

	case openTopicMsg:
		m.page = pageDetail
		m.detailBack = msg.from
		m.detail = newDetailModel(m.deps, msg.topic)
		m.detail, _ = m.detail.resize(m.width, m.height-3)
		return m, m.detail.Init()

	case topicMappingLoadedMsg:
		var cmd tea.Cmd
		m.topicMapping, cmd = m.topicMapping.update(msg)
		return m, cmd

	case protoMessageSelectedMsg:
		m.page = pageProtoSchema
		m.protoSchema = newProtoSchemaModel(m.deps, msg.fqn)
		m.protoSchema, _ = m.protoSchema.resize(m.width, m.height-2)
		return m, nil

	case asyncMessageSelectedMsg:
		m.page = pageAsyncDetail
		m.asyncDetail = newAsyncDetailModel(m.deps, msg.topic, msg.idx)
		m.asyncDetail = m.asyncDetail.resize(m.width, m.height-2)
		return m, nil

	case cacheTopicAddedMsg:
		m.page = pageKafkaCache
		m.cacheList = newCacheListModel(m.deps)
		m.cacheList, _ = m.cacheList.resize(m.width, m.height-2)
		return m, m.cacheList.Init()

	case batchProgressMsg, batchFinishedMsg, topicsLoadedMsg:
		// Keep the bulk sample/estimate job progressing (and its status
		// visible, and the list refreshed once it finishes) no matter which
		// page is active, so navigating into a topic while "S"/"E"/"A" is
		// running doesn't silently stall or lose it.
		if _, ok := msg.(topicsLoadedMsg); ok {
			m.topicsReady = true
			m.leaveSplashIfReady()
		}
		var cmd tea.Cmd
		m.list, cmd = m.list.update(m.ctx, msg)
		return m, cmd

	case kafkaSettingsAppliedMsg:
		var cmd tea.Cmd
		m.kafkaSettings, cmd = m.kafkaSettings.update(msg)
		if msg.err == nil {
			// Remember what was applied so leaving and re-entering the
			// screen shows it, and reload topics from the new cluster.
			m.deps.KafkaSettings = msg.input
			m.list.loading = true
			return m, tea.Batch(cmd, m.list.Init())
		}
		return m, cmd

	case schemaSourcesLoadedMsg:
		if msg.protoErr == nil {
			m.deps.ProtoImportPaths = append([]string(nil), msg.protoPaths...)
			m.deps.ProtoIncludePaths = append([]string(nil), msg.includePaths...)
			m.deps.Messages = msg.messages
			if m.deps.SyncManager != nil {
				m.deps.SyncManager.SetImportPaths(msg.protoPaths, msg.includePaths)
			}
		}
		m.deps.AsyncAPIPaths = append([]string(nil), msg.asyncPaths...)
		m.deps.AsyncAPI = msg.index
		m.protoMessages = newProtoMessagesModel(m.deps)
		m.protoMessages, _ = m.protoMessages.resize(m.width, m.height-2)
		var cmd tea.Cmd
		m.protoSettings, cmd = m.protoSettings.update(msg)
		return m, cmd

	case menuOpenDuckDBUIMsg:
		switch {
		case m.deps.DB == nil:
			m.menu.note, m.menu.noteIsOK = "the DuckDB UI needs a database file (DUCKDB_PATH is :memory:)", false
		case !m.deps.DuckDBUIRunning:
			m.menu.note, m.menu.noteIsOK = "the DuckDB UI is not running: press enter to enable it", false
		default:
			return m, openBrowserCmd(m.deps.DuckDBUI.URL())
		}
		return m, nil

	case browserOpenedMsg:
		if m.page == pageDuckDBUISettings {
			var cmd tea.Cmd
			m.duckDBUI, cmd = m.duckDBUI.update(msg)
			return m, cmd
		}
		if msg.err != nil {
			m.menu.note, m.menu.noteIsOK = msg.err.Error()+" — open "+msg.url+" manually", false
		} else {
			m.menu.note, m.menu.noteIsOK = "opened "+msg.url+" in your browser", true
		}
		return m, nil

	case duckDBUIAppliedMsg:
		if msg.err == nil {
			m.deps.DuckDBUI = msg.settings
		}
		m.deps.DuckDBUIRunning = msg.running
		m.deps.DuckDBUIErr = ""
		var cmd tea.Cmd
		m.duckDBUI, cmd = m.duckDBUI.update(msg)
		m.menu.duckDBUISummary = m.duckDBUI.menuSummary()
		return m, cmd

	case tea.KeyboardEnhancementsMsg:
		// Sent once at startup: remember it for the REPL.
		m.keyboardEnhanced = msg.SupportsKeyDisambiguation()
		m.sqlRepl.enhanced = m.keyboardEnhanced
		return m, nil

	case sqlRunDoneMsg, sqlCompleteMsg:
		// REPL results arrive even if the user left the screen meanwhile.
		if !m.sqlReplOpen {
			return m, nil
		}
		var cmd tea.Cmd
		m.sqlRepl, cmd = m.sqlRepl.update(msg)
		return m, cmd

	case sqlLeaveMsg:
		m.page = pageMenu
		return m, nil

	case splashDoneMsg:
		m.splashElapsed = true
		m.leaveSplashIfReady()
		return m, nil

	case menuTickMsg:
		if m.deps.SyncManager != nil {
			active, disk := m.deps.SyncManager.TotalActive(m.ctx)
			m.menu.cacheSummary = fmt.Sprintf("(%d syncs: %s)", active, humanBytes(disk))
		}
		return m, menuAutoRefreshCmd()

	case spinner.TickMsg:
		if m.page == pageSplash {
			var cmd tea.Cmd
			m.splash, cmd = m.splash.update(msg)
			return m, cmd
		}
		if m.page != pageList && m.list.batchRunning {
			// The list's batch spinner must keep ticking even while another
			// page is active; forward to both so a same-named tick destined
			// for the active page's own spinner is not lost.
			var listCmd tea.Cmd
			m.list, listCmd = m.list.update(m.ctx, msg)
			rest, cmd := m.updatePage(msg)
			return rest, tea.Batch(listCmd, cmd)
		}
	}

	return m.updatePage(msg)
}

// leaveSplashIfReady moves from the splash to the main menu once the minimum
// display time has elapsed and topics have loaded.
func (m *Model) leaveSplashIfReady() {
	if m.page == pageSplash && m.splashElapsed && m.topicsReady {
		m.page = pageMenu
	}
}

func (m Model) updatePage(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch m.page {
	case pageSplash:
		return m, nil
	case pageMenu:
		return m.updateMenu(msg)
	case pageList:
		return m.updateList(msg)
	case pageDetail:
		return m.updateDetail(msg)
	case pageGroups:
		return m.updateGroups(msg)
	case pageGroupDetail:
		return m.updateGroupDetail(msg)
	case pageKafkaCache:
		return m.updateCacheList(msg)
	case pageCacheAdd:
		return m.updateCacheAdd(msg)
	case pageCacheDetail:
		return m.updateCacheDetail(msg)
	case pageKafkaSettings:
		return m.updateKafkaSettings(msg)
	case pageProtoMessages:
		return m.updateProtoMessages(msg)
	case pageProtoSettings:
		return m.updateProtoSettings(msg)
	case pageDuckDBUISettings:
		return m.updateDuckDBUISettings(msg)
	case pageTopicMapping:
		return m.updateTopicMapping(msg)
	case pageProtoSchema:
		return m.updateProtoSchema(msg)
	case pageAsyncDetail:
		return m.updateAsyncDetail(msg)
	case pageSchemaPicker:
		return m.updateSchemaPicker(msg)
	case pageSQLRepl:
		return m.updateSQLRepl(msg)
	}
	return m, nil
}

func (m Model) updateMenu(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyPressMsg); ok && key.String() == "q" {
		return m, tea.Quit
	}
	if m.deps.SyncManager != nil {
		active, disk := m.deps.SyncManager.TotalActive(m.ctx)
		m.menu.cacheSummary = fmt.Sprintf("(%d syncs: %s)", active, humanBytes(disk))
	}
	var cmd tea.Cmd
	m.menu, cmd = m.menu.update(msg)
	return m, cmd
}

func (m Model) updateList(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyPressMsg); ok && !m.list.filtering() {
		switch key.String() {
		case "esc":
			if !m.list.batchRunning && m.list.filter.Value() == "" {
				m.page = pageMenu
				return m, nil
			}
		case "g":
			m.page = pageGroups
			m.groups = newGroupsModel(m.deps)
			m.groups, _ = m.groups.resize(m.width, m.height-2)
			return m, m.groups.Init()
		case "enter":
			if topic, ok := m.list.selectedTopic(); ok {
				m.page = pageDetail
				m.detailBack = pageList
				m.detail = newDetailModel(m.deps, topic)
				m.detail, _ = m.detail.resize(m.width, m.height-3)
				return m, m.detail.Init()
			}
		case "m":
			if topic, ok := m.list.selectedTopic(); ok && !m.list.batchRunning {
				return m.openSchemaPicker(topic)
			}
		}
	}
	var cmd tea.Cmd
	m.list, cmd = m.list.update(m.ctx, msg)
	return m, cmd
}

func (m Model) updateDetail(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyPressMsg); ok && !m.detail.filtering() {
		switch key.String() {
		case "esc":
			m.detail.stopTail()
			m.page = pageList
			if m.detailBack == pageTopicMapping {
				m.page = pageTopicMapping
			}
			m.detailBack = pageList
			return m, nil
		case "m":
			return m.openSchemaPicker(m.detail.topic)
		}
	}
	var cmd tea.Cmd
	m.detail, cmd = m.detail.update(m.ctx, msg)
	return m, cmd
}

func (m Model) updateCacheList(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyPressMsg); ok && !m.cacheList.filterOn && !m.cacheList.confirmDrop {
		switch key.String() {
		case "esc":
			if m.cacheList.filter.Value() == "" {
				m.page = pageMenu
				return m, nil
			}
		case "a":
			m.page = pageCacheAdd
			m.cacheAdd = newCacheAddModel(m.deps)
			m.cacheAdd = m.cacheAdd.resize(m.width, m.height-2)
			return m, m.cacheAdd.Init()
		case "enter":
			if topic, ok := m.cacheList.selectedTopic(); ok {
				m.page = pageCacheDetail
				m.cacheDetail = newCacheDetailModel(m.deps, topic)
				m.cacheDetail = m.cacheDetail.resize(m.width, m.height-2)
				return m, m.cacheDetail.Init()
			}
		}
	}
	var cmd tea.Cmd
	m.cacheList, cmd = m.cacheList.update(m.ctx, msg)
	return m, cmd
}

func (m Model) updateCacheAdd(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	m.cacheAdd, cmd = m.cacheAdd.update(m.ctx, msg)
	return m, cmd
}

func (m Model) updateCacheDetail(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyPressMsg); ok && key.String() == "esc" {
		m.page = pageKafkaCache
		return m, nil
	}
	var cmd tea.Cmd
	m.cacheDetail, cmd = m.cacheDetail.update(msg)
	return m, cmd
}

func (m Model) updateGroups(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyPressMsg); ok {
		switch key.String() {
		case "esc":
			m.page = pageMenu
			return m, nil
		case "enter":
			if group, ok := m.groups.selectedGroup(); ok {
				m.page = pageGroupDetail
				m.groupDetail = newGroupDetailModel(m.deps, group)
				m.groupDetail, _ = m.groupDetail.resize(m.width, m.height-2)
				return m, m.groupDetail.Init()
			}
		}
	}
	var cmd tea.Cmd
	m.groups, cmd = m.groups.update(m.ctx, msg)
	return m, cmd
}

func (m Model) updateGroupDetail(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyPressMsg); ok && key.String() == "esc" {
		m.page = pageGroups
		return m, nil
	}
	var cmd tea.Cmd
	m.groupDetail, cmd = m.groupDetail.update(m.ctx, msg)
	return m, cmd
}

func (m Model) updateKafkaSettings(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyPressMsg); ok && key.String() == "esc" && !m.kafkaSettings.capturingInput() {
		m.page = pageMenu
		// Leaving discards unapplied edits.
		m.kafkaSettings = newKafkaSettingsModel(m.ctx, m.deps)
		return m, nil
	}
	var cmd tea.Cmd
	m.kafkaSettings, cmd = m.kafkaSettings.update(msg)
	return m, cmd
}

func (m Model) updateDuckDBUISettings(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyPressMsg); ok && key.String() == "esc" && !m.duckDBUI.capturingInput() {
		m.page = pageMenu
		// Leaving discards unapplied edits.
		m.duckDBUI = newDuckDBUISettingsModel(m.ctx, m.deps)
		return m, nil
	}
	var cmd tea.Cmd
	m.duckDBUI, cmd = m.duckDBUI.update(msg)
	return m, cmd
}

func (m Model) updateProtoMessages(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyPressMsg); ok && key.String() == "esc" &&
		!m.protoMessages.filtering() && m.protoMessages.filter.Value() == "" {
		m.page = pageMenu
		return m, nil
	}
	var cmd tea.Cmd
	m.protoMessages, cmd = m.protoMessages.update(msg)
	return m, cmd
}

func (m Model) updateProtoSettings(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyPressMsg); ok && key.String() == "esc" && !m.protoSettings.capturingInput() {
		m.page = pageMenu
		// Leaving discards unapplied edits.
		m.protoSettings = newProtoSettingsModel(m.ctx, m.deps)
		return m, nil
	}
	var cmd tea.Cmd
	m.protoSettings, cmd = m.protoSettings.update(msg)
	return m, cmd
}

// openSchemaPicker shows the message-override picker for topic; esc
// returns to the page it was opened from.
func (m Model) openSchemaPicker(topic string) (tea.Model, tea.Cmd) {
	m.pickerReturn = m.page
	m.page = pageSchemaPicker
	m.schemaPicker = newSchemaPickerModel(m.deps, topic)
	m.schemaPicker = m.schemaPicker.resize(m.width, m.height-2)
	return m, m.schemaPicker.Init()
}

func (m Model) updateSchemaPicker(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyPressMsg); ok && key.String() == "esc" {
		m.page = m.pickerReturn
		return m, nil
	}
	var cmd tea.Cmd
	m.schemaPicker, cmd = m.schemaPicker.update(msg)
	return m, cmd
}

func (m Model) updateProtoSchema(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyPressMsg); ok && key.String() == "esc" {
		m.page = pageProtoMessages
		return m, nil
	}
	var cmd tea.Cmd
	m.protoSchema, cmd = m.protoSchema.update(msg)
	return m, cmd
}

func (m Model) updateAsyncDetail(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyPressMsg); ok && key.String() == "esc" {
		m.page = pageProtoMessages
		return m, nil
	}
	var cmd tea.Cmd
	m.asyncDetail, cmd = m.asyncDetail.update(msg)
	return m, cmd
}

// updateSQLRepl routes to the REPL; esc (when the REPL is not using it)
// returns to the menu, keeping the session, scrollback and settings for
// the next visit.
func (m Model) updateSQLRepl(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyPressMsg); ok && key.String() == "esc" && !m.sqlRepl.capturingInput() {
		m.page = pageMenu
		return m, nil
	}
	var cmd tea.Cmd
	m.sqlRepl, cmd = m.sqlRepl.update(msg)
	return m, cmd
}

// Close releases resources held by screens (the REPL's connections). Call
// it after the program exits.
func (m Model) Close() {
	if m.sqlReplOpen {
		m.sqlRepl.close()
	}
}

func (m Model) View() tea.View {
	if m.page == pageSplash {
		v := tea.NewView(m.splash.View())
		v.AltScreen = true
		return v
	}
	if m.page == pageMenu {
		v := tea.NewView(m.menu.View())
		v.AltScreen = true
		return v
	}

	var body string
	switch m.page {
	case pageList:
		body = m.list.View()
	case pageDetail:
		body = m.detail.View()
	case pageGroups:
		body = m.groups.View()
	case pageGroupDetail:
		body = m.groupDetail.View()
	case pageKafkaCache:
		body = m.cacheList.View()
	case pageCacheAdd:
		body = m.cacheAdd.View()
	case pageCacheDetail:
		body = m.cacheDetail.View()
	case pageKafkaSettings:
		body = m.kafkaSettings.View()
	case pageProtoMessages:
		body = m.protoMessages.View()
	case pageProtoSettings:
		body = m.protoSettings.View()
	case pageDuckDBUISettings:
		body = m.duckDBUI.View()
	case pageTopicMapping:
		body = m.topicMapping.View()
	case pageProtoSchema:
		body = m.protoSchema.View()
	case pageAsyncDetail:
		body = m.asyncDetail.View()
	case pageSchemaPicker:
		body = m.schemaPicker.View()
	case pageSQLRepl:
		body = m.sqlRepl.View()
	}

	title := "bufka tui"
	switch m.page {
	case pageDetail:
		title = fmt.Sprintf("bufka tui — %s", m.detail.topic)
	case pageGroups:
		title = "bufka tui — consumer groups"
	case pageGroupDetail:
		title = fmt.Sprintf("bufka tui — group %s", m.groupDetail.group)
	case pageKafkaCache:
		title = "bufka tui — kafka cache"
	case pageCacheAdd:
		title = "bufka tui — add topic to cache"
	case pageCacheDetail:
		title = fmt.Sprintf("bufka tui — cache %s", m.cacheDetail.topic)
	case pageKafkaSettings:
		title = "bufka tui — kafka connection"
	case pageProtoMessages:
		title = "bufka tui — schemas — catalog [" + m.protoMessages.kindLabel() + "]"
	case pageProtoSettings:
		title = "bufka tui — schema sources"
	case pageDuckDBUISettings:
		title = "bufka tui — duckdb ui"
	case pageTopicMapping:
		title = "bufka tui — topic mapping"
	case pageProtoSchema:
		title = fmt.Sprintf("bufka tui — %s", m.protoSchema.fqn)
	case pageAsyncDetail:
		title = fmt.Sprintf("bufka tui — %s", m.asyncDetail.title())
	case pageSchemaPicker:
		title = fmt.Sprintf("bufka tui — %s: choose message schema", m.schemaPicker.topic)
	case pageSQLRepl:
		title = "bufka tui — sql repl"
	}

	bar := m.status
	if m.err != nil {
		bar = m.err.Error()
	}
	var barView string
	if m.err != nil {
		barView = errorStyle.Render(bar)
	} else {
		barView = statusStyle.Render(bar)
	}

	v := tea.NewView(titleStyle.Width(m.width).Render(title) + "\n" + body + "\n" + barView)
	v.AltScreen = true
	if m.page == pageSQLRepl {
		// The alternate screen has no terminal scrollback: report the wheel
		// so the REPL can scroll its own output.
		v.MouseMode = tea.MouseModeCellMotion
	}
	return v
}

// statusMsg sets the status bar text.
type statusMsg string

// errMsg surfaces an adapter error in the status bar instead of crashing.
type errMsg struct{ err error }

func setStatus(s string) tea.Cmd {
	return func() tea.Msg { return statusMsg(s) }
}

func setErr(err error) tea.Cmd {
	return func() tea.Msg { return errMsg{err} }
}

// withTimeout runs fn with a context bounded by timeout derived from ctx.
func withTimeout(ctx context.Context, timeout time.Duration, fn func(context.Context) tea.Msg) tea.Cmd {
	return func() tea.Msg {
		c, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return fn(c)
	}
}
