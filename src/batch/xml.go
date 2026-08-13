package batch

import (
	"encoding/xml"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/src/cache"
	"github.com/dsub-io/go-open-discogs-batch/src/dateparser"
	"github.com/dsub-io/go-open-discogs-batch/src/helper"
	opendiscogsmodel "github.com/dsub-io/open-discogs-model/model"
)

const (
	releaseFormatHashFieldSeparator = "\x00"
	releaseFormatHashNullValue      = "\x01"
	relationOrdinalOutOfRange       = "relation ordinal exceeds PostgreSQL integer range"
)

func relationOrdinal(index int) *int32 {
	if index < 0 || int64(index) > int64(math.MaxInt32) {
		panic(relationOrdinalOutOfRange)
	}
	ordinal := int32(index)
	return &ordinal
}

type XmlRef struct {
	ID   int32  `xml:"id,attr"`
	Name string `xml:",chardata"`
}

type XmlArtist struct {
	ID          int32   `xml:"id"`
	Name        *string `xml:"name"`
	DataQuality *string `xml:"data_quality"`
	Profile     *string `xml:"profile"`
	RealName    *string `xml:"realname"`
}

func (a *XmlArtist) TransformAt(observedAt time.Time) *opendiscogsmodel.Artist {
	return &opendiscogsmodel.Artist{
		ID:             a.ID,
		CreatedAt:      observedAt,
		LastModifiedAt: observedAt,
		DataQuality:    helper.FilterStr(a.DataQuality),
		Name:           helper.FilterStr(a.Name),
		Profile:        helper.FilterStr(a.Profile),
		RealName:       helper.FilterStr(a.RealName),
	}
}

type XmlArtistRelation struct {
	ID          int32    `xml:"id"`
	Name        *string  `xml:"name"`
	DataQuality *string  `xml:"data_quality"`
	Profile     *string  `xml:"profile"`
	RealName    *string  `xml:"realname"`
	URLs        []string `xml:"urls>url"`
	NameVars    []string `xml:"namevariations>name"`
	Aliases     []XmlRef `xml:"aliases>name"`
	Groups      []XmlRef `xml:"groups>name"`
	Members     []XmlRef `xml:"members>name"`
	observedAt  time.Time
}

func (a *XmlArtistRelation) setObservedAt(observedAt time.Time) {
	if a == nil {
		return
	}
	a.observedAt = observedAt
}

func (a *XmlArtistRelation) timestamp() time.Time {
	if a.observedAt.IsZero() {
		a.observedAt = time.Now().UTC()
	}
	return a.observedAt
}

func (a *XmlArtistRelation) GetArtist() *opendiscogsmodel.Artist {
	observedAt := a.timestamp()
	return &opendiscogsmodel.Artist{
		ID:             a.ID,
		CreatedAt:      observedAt,
		LastModifiedAt: observedAt,
		DataQuality:    helper.FilterStr(a.DataQuality),
		Name:           helper.FilterStr(a.Name),
		Profile:        helper.FilterStr(a.Profile),
		RealName:       helper.FilterStr(a.RealName),
	}
}

func (a *XmlArtistRelation) GetUrls() []*opendiscogsmodel.ArtistURL {
	observedAt := a.timestamp()
	items := make([]*opendiscogsmodel.ArtistURL, 0, len(a.URLs))
	for index, value := range a.URLs {
		url := strings.TrimSpace(value)
		if url == "" {
			continue
		}
		items = append(items, &opendiscogsmodel.ArtistURL{
			ArtistID:       a.ID,
			Ordinal:        relationOrdinal(index),
			Hash:           helper.JavaStringHash(url),
			URL:            url,
			LastModifiedAt: observedAt,
		})
	}
	return items
}

func (a *XmlArtistRelation) GetNameVars() []*opendiscogsmodel.ArtistNameVariation {
	observedAt := a.timestamp()
	items := make([]*opendiscogsmodel.ArtistNameVariation, 0, len(a.NameVars))
	for index, value := range a.NameVars {
		nameVariation := strings.TrimSpace(value)
		if nameVariation == "" {
			continue
		}
		items = append(items, &opendiscogsmodel.ArtistNameVariation{
			ArtistID:       a.ID,
			Ordinal:        relationOrdinal(index),
			NameVariation:  nameVariation,
			Hash:           helper.JavaStringHash(nameVariation),
			LastModifiedAt: observedAt,
		})
	}
	return items
}

func (a *XmlArtistRelation) GetAliases() []*opendiscogsmodel.ArtistAlias {
	observedAt := a.timestamp()
	items := make([]*opendiscogsmodel.ArtistAlias, 0, len(a.Aliases))
	for index, alias := range a.Aliases {
		if !cache.ArtistIDs.Contains(alias.ID) {
			continue
		}
		items = append(items, &opendiscogsmodel.ArtistAlias{
			ArtistID:       a.ID,
			AliasID:        alias.ID,
			Ordinal:        relationOrdinal(index),
			LastModifiedAt: observedAt,
		})
	}
	return items
}

func (a *XmlArtistRelation) GetGroups() []*opendiscogsmodel.ArtistGroup {
	observedAt := a.timestamp()
	items := make([]*opendiscogsmodel.ArtistGroup, 0, len(a.Groups))
	for index, group := range a.Groups {
		if !cache.ArtistIDs.Contains(group.ID) {
			continue
		}
		items = append(items, &opendiscogsmodel.ArtistGroup{
			ArtistID:       a.ID,
			GroupID:        group.ID,
			Ordinal:        relationOrdinal(index),
			LastModifiedAt: observedAt,
		})
	}
	return items
}

func (a *XmlArtistRelation) GetMembers() []*opendiscogsmodel.ArtistMember {
	observedAt := a.timestamp()
	items := make([]*opendiscogsmodel.ArtistMember, 0, len(a.Members))
	for index, member := range a.Members {
		if !cache.ArtistIDs.Contains(member.ID) {
			continue
		}
		items = append(items, &opendiscogsmodel.ArtistMember{
			ArtistID:       a.ID,
			MemberID:       member.ID,
			Ordinal:        relationOrdinal(index),
			LastModifiedAt: observedAt,
		})
	}
	return items
}

type XmlLabel struct {
	ID          int32   `xml:"id"`
	Name        *string `xml:"name"`
	ContactInfo *string `xml:"contactinfo"`
	Profile     *string `xml:"profile"`
	DataQuality *string `xml:"data_quality"`
}

func (l *XmlLabel) TransformAt(observedAt time.Time) *opendiscogsmodel.Label {
	return &opendiscogsmodel.Label{
		ID:             l.ID,
		CreatedAt:      observedAt,
		LastModifiedAt: observedAt,
		Name:           helper.FilterStr(l.Name),
		ContactInfo:    helper.FilterStr(l.ContactInfo),
		Profile:        helper.FilterStr(l.Profile),
		DataQuality:    helper.FilterStr(l.DataQuality),
	}
}

type XmlLabelRelation struct {
	ID          int32    `xml:"id"`
	Name        *string  `xml:"name"`
	ContactInfo *string  `xml:"contactinfo"`
	Profile     *string  `xml:"profile"`
	DataQuality *string  `xml:"data_quality"`
	URLs        []string `xml:"urls>url"`
	SubLabels   []XmlRef `xml:"sublabels>label"`
	observedAt  time.Time
}

func (l *XmlLabelRelation) setObservedAt(observedAt time.Time) {
	if l == nil {
		return
	}
	l.observedAt = observedAt
}

func (l *XmlLabelRelation) timestamp() time.Time {
	if l.observedAt.IsZero() {
		l.observedAt = time.Now().UTC()
	}
	return l.observedAt
}

func (l *XmlLabelRelation) GetLabel() *opendiscogsmodel.Label {
	observedAt := l.timestamp()
	return &opendiscogsmodel.Label{
		ID:             l.ID,
		CreatedAt:      observedAt,
		LastModifiedAt: observedAt,
		Name:           helper.FilterStr(l.Name),
		ContactInfo:    helper.FilterStr(l.ContactInfo),
		Profile:        helper.FilterStr(l.Profile),
		DataQuality:    helper.FilterStr(l.DataQuality),
	}
}

func (l *XmlLabelRelation) GetUrls() []*opendiscogsmodel.LabelURL {
	observedAt := l.timestamp()
	items := make([]*opendiscogsmodel.LabelURL, 0, len(l.URLs))
	for index, value := range l.URLs {
		url := strings.TrimSpace(value)
		if url == "" {
			continue
		}
		items = append(items, &opendiscogsmodel.LabelURL{
			LabelID:        l.ID,
			Ordinal:        relationOrdinal(index),
			Hash:           helper.JavaStringHash(url),
			URL:            url,
			LastModifiedAt: observedAt,
		})
	}
	return items
}

func (l *XmlLabelRelation) GetSubLabels() []*opendiscogsmodel.LabelSubLabel {
	observedAt := l.timestamp()
	items := make([]*opendiscogsmodel.LabelSubLabel, 0, len(l.SubLabels))
	for index, subLabel := range l.SubLabels {
		if !cache.LabelIDs.Contains(subLabel.ID) {
			continue
		}
		items = append(items, &opendiscogsmodel.LabelSubLabel{
			ParentLabelID:  l.ID,
			SubLabelID:     subLabel.ID,
			Ordinal:        relationOrdinal(index),
			LastModifiedAt: observedAt,
		})
	}
	return items
}

type XmlMaster struct {
	ID            int32   `xml:"id,attr"`
	MainReleaseID *int32  `xml:"main_release"`
	Title         *string `xml:"title"`
	DataQuality   *string `xml:"data_quality"`
	Year          *int16  `xml:"year"`
}

type XmlGenreStyle struct {
	Styles []string `xml:"styles>style"`
	Genres []string `xml:"genres>genre"`
}

func (m *XmlMaster) TransformAt(observedAt time.Time) *opendiscogsmodel.Master {
	return &opendiscogsmodel.Master{
		ID:             m.ID,
		CreatedAt:      observedAt,
		LastModifiedAt: observedAt,
		Title:          helper.FilterStr(m.Title),
		DataQuality:    helper.FilterStr(m.DataQuality),
		Year:           m.Year,
	}
}

type XmlMasterRelation struct {
	ID            int32      `xml:"id,attr"`
	MainReleaseID *int32     `xml:"main_release"`
	Title         *string    `xml:"title"`
	DataQuality   *string    `xml:"data_quality"`
	Year          *int16     `xml:"year"`
	Styles        []string   `xml:"styles>style"`
	Genres        []string   `xml:"genres>genre"`
	Artists       []int32    `xml:"artists>artist>id"`
	Videos        []XmlVideo `xml:"videos>video"`
	observedAt    time.Time
}

func (m *XmlMasterRelation) setObservedAt(observedAt time.Time) {
	if m == nil {
		return
	}
	m.observedAt = observedAt
}

func (m *XmlMasterRelation) timestamp() time.Time {
	if m.observedAt.IsZero() {
		m.observedAt = time.Now().UTC()
	}
	return m.observedAt
}

type XmlVideo struct {
	URL         string  `xml:"src,attr"`
	Title       *string `xml:"title"`
	Description *string `xml:"description"`
}

func (m *XmlMasterRelation) GetStyles() []*opendiscogsmodel.Style {
	return styles(m.Styles)
}

func (m *XmlMasterRelation) GetGenres() []*opendiscogsmodel.Genre {
	return genres(m.Genres)
}

func (m *XmlMasterRelation) GetMaster() *opendiscogsmodel.Master {
	observedAt := m.timestamp()
	return &opendiscogsmodel.Master{
		ID:             m.ID,
		CreatedAt:      observedAt,
		LastModifiedAt: observedAt,
		Title:          helper.FilterStr(m.Title),
		DataQuality:    helper.FilterStr(m.DataQuality),
		Year:           m.Year,
	}
}

func (m *XmlMasterRelation) GetMasterStyles() []*opendiscogsmodel.MasterStyle {
	observedAt := m.timestamp()
	items := make([]*opendiscogsmodel.MasterStyle, 0, len(m.Styles))
	for index, value := range m.Styles {
		style := strings.TrimSpace(value)
		if style == "" {
			continue
		}
		items = append(items, &opendiscogsmodel.MasterStyle{
			MasterID:       m.ID,
			Ordinal:        relationOrdinal(index),
			Style:          style,
			LastModifiedAt: observedAt,
		})
	}
	return items
}

func (m *XmlMasterRelation) GetMasterGenres() []*opendiscogsmodel.MasterGenre {
	observedAt := m.timestamp()
	items := make([]*opendiscogsmodel.MasterGenre, 0, len(m.Genres))
	for index, value := range m.Genres {
		genre := strings.TrimSpace(value)
		if genre == "" {
			continue
		}
		items = append(items, &opendiscogsmodel.MasterGenre{
			MasterID:       m.ID,
			Ordinal:        relationOrdinal(index),
			Genre:          genre,
			LastModifiedAt: observedAt,
		})
	}
	return items
}

func (m *XmlMasterRelation) GetMasterVideos() []*opendiscogsmodel.MasterVideo {
	observedAt := m.timestamp()
	items := make([]*opendiscogsmodel.MasterVideo, 0, len(m.Videos))
	for index, video := range m.Videos {
		title := helper.FilterStr(video.Title)
		description := helper.FilterStr(video.Description)
		url := helper.FilterStr(&video.URL)
		hashSource := stringValue(title) + stringValue(description) + stringValue(url)
		if strings.TrimSpace(hashSource) == "" {
			continue
		}
		items = append(items, &opendiscogsmodel.MasterVideo{
			MasterID:       m.ID,
			Ordinal:        relationOrdinal(index),
			Hash:           helper.JavaStringHash(hashSource),
			URL:            url,
			Description:    description,
			Title:          title,
			LastModifiedAt: observedAt,
		})
	}
	return items
}

func (m *XmlMasterRelation) GetMasterArtists() []*opendiscogsmodel.MasterArtist {
	observedAt := m.timestamp()
	items := make([]*opendiscogsmodel.MasterArtist, 0, len(m.Artists))
	for index, artistID := range m.Artists {
		if !cache.ArtistIDs.Contains(artistID) {
			continue
		}
		items = append(items, &opendiscogsmodel.MasterArtist{
			ArtistID:       artistID,
			MasterID:       m.ID,
			Ordinal:        relationOrdinal(index),
			LastModifiedAt: observedAt,
		})
	}
	return items
}

type XmlRelease struct {
	ID                int32                `xml:"id,attr"`
	Title             *string              `xml:"title"`
	Country           *string              `xml:"country"`
	DataQuality       *string              `xml:"data_quality"`
	ListedReleaseDate *string              `xml:"released"`
	Notes             *string              `xml:"notes"`
	MasterInfo        XmlReleaseMasterInfo `xml:"master_id"`
	Status            *string              `xml:"status,attr"`
}

type XmlReleaseMasterInfo struct {
	MasterID *int32 `xml:",chardata"`
	IsMaster bool   `xml:"is_main_release,attr"`
}

func (r *XmlRelease) TransformAt(observedAt time.Time) *opendiscogsmodel.ReleaseItem {
	return releaseItem(
		r.ID,
		r.Title,
		r.Country,
		r.DataQuality,
		r.ListedReleaseDate,
		r.Notes,
		r.MasterInfo,
		r.Status,
		observedAt,
	)
}

type XmlLabelRelease struct {
	LabelID          int32  `xml:"id,attr"`
	CategoryNotation string `xml:"catno,attr"`
}

type XmlCreditedArtist struct {
	ArtistID int32  `xml:"id"`
	Role     string `xml:"role"`
}

type XmlFormat struct {
	Name         *string           `xml:"name,attr"`
	Quantity     XmlFormatQuantity `xml:"qty,attr"`
	Text         *string           `xml:"text,attr"`
	Descriptions []string          `xml:"descriptions>description"`
}

type XmlFormatQuantity struct {
	canonical string
	integer   *int32
	present   bool
}

func (quantity *XmlFormatQuantity) UnmarshalXMLAttr(attribute xml.Attr) error {
	normalized := strings.TrimSpace(attribute.Value)
	if normalized == "" {
		return nil
	}
	canonical, integer, err := canonicalReleaseFormatQuantity(normalized)
	if err != nil {
		return fmt.Errorf("invalid non-negative release format quantity %q", attribute.Value)
	}
	quantity.canonical = canonical
	quantity.present = true
	quantity.integer = integer
	return nil
}

func (quantity XmlFormatQuantity) Canonical() *string {
	if !quantity.present {
		return nil
	}
	value := quantity.canonical
	return &value
}

func (quantity XmlFormatQuantity) Integer() *int32 {
	if quantity.integer == nil {
		return nil
	}
	value := *quantity.integer
	return &value
}

func newXmlFormatQuantity(value int32) XmlFormatQuantity {
	canonical := strconv.FormatInt(int64(value), 10)
	return XmlFormatQuantity{canonical: canonical, integer: &value, present: true}
}

type XmlTrack struct {
	Position string `xml:"position"`
	Title    string `xml:"title"`
	Duration string `xml:"duration"`
}

type XmlIdentifier struct {
	Type        string `xml:"type,attr"`
	Description string `xml:"description,attr"`
	Value       string `xml:"value,attr"`
}

type XmlWork struct {
	LabelID int32  `xml:"id"`
	Work    string `xml:"entity_type_name"`
}

type XmlReleaseRelation struct {
	ID                int32                `xml:"id,attr"`
	Title             *string              `xml:"title"`
	Country           *string              `xml:"country"`
	DataQuality       *string              `xml:"data_quality"`
	ListedReleaseDate *string              `xml:"released"`
	Notes             *string              `xml:"notes"`
	MasterInfo        XmlReleaseMasterInfo `xml:"master_id"`
	Status            *string              `xml:"status,attr"`
	Artists           []int32              `xml:"artists>artist>id"`
	Labels            []XmlLabelRelease    `xml:"labels>label"`
	CreditedArtists   []XmlCreditedArtist  `xml:"extraartists>artist"`
	Formats           []XmlFormat          `xml:"formats>format"`
	Genres            []string             `xml:"genres>genre"`
	Styles            []string             `xml:"styles>style"`
	Tracks            []XmlTrack           `xml:"tracklist>track"`
	Identifiers       []XmlIdentifier      `xml:"identifiers>identifier"`
	Videos            []XmlVideo           `xml:"videos>video"`
	Works             []XmlWork            `xml:"companies>company"`
	observedAt        time.Time
}

func (r *XmlReleaseRelation) setObservedAt(observedAt time.Time) {
	if r == nil {
		return
	}
	r.observedAt = observedAt
}

func (r *XmlReleaseRelation) timestamp() time.Time {
	if r.observedAt.IsZero() {
		r.observedAt = time.Now().UTC()
	}
	return r.observedAt
}

func (r *XmlReleaseRelation) GetGenres() []*opendiscogsmodel.Genre {
	return genres(r.Genres)
}

func (r *XmlReleaseRelation) GetStyles() []*opendiscogsmodel.Style {
	return styles(r.Styles)
}

func (r *XmlReleaseRelation) GetRelease() *opendiscogsmodel.ReleaseItem {
	observedAt := r.timestamp()
	return releaseItem(
		r.ID,
		r.Title,
		r.Country,
		r.DataQuality,
		r.ListedReleaseDate,
		r.Notes,
		r.MasterInfo,
		r.Status,
		observedAt,
	)
}

func (r *XmlReleaseRelation) GetWorks() []*opendiscogsmodel.ReleaseItemWork {
	observedAt := r.timestamp()
	items := make([]*opendiscogsmodel.ReleaseItemWork, 0, len(r.Works))
	for index, work := range r.Works {
		value := strings.TrimSpace(work.Work)
		if value == "" {
			continue
		}
		if !cache.LabelIDs.Contains(work.LabelID) {
			continue
		}
		items = append(items, &opendiscogsmodel.ReleaseItemWork{
			ReleaseItemID:  r.ID,
			LabelID:        work.LabelID,
			Ordinal:        relationOrdinal(index),
			Work:           &value,
			Hash:           helper.JavaStringHash(value),
			LastModifiedAt: observedAt,
		})
	}
	return items
}

func (r *XmlReleaseRelation) GetVideos() []*opendiscogsmodel.ReleaseItemVideo {
	observedAt := r.timestamp()
	items := make([]*opendiscogsmodel.ReleaseItemVideo, 0, len(r.Videos))
	for index, video := range r.Videos {
		title := helper.FilterStr(video.Title)
		description := helper.FilterStr(video.Description)
		url := helper.FilterStr(&video.URL)
		hashSource := stringValue(title) + stringValue(description) + stringValue(url)
		if strings.TrimSpace(hashSource) == "" {
			continue
		}
		items = append(items, &opendiscogsmodel.ReleaseItemVideo{
			ReleaseItemID:  r.ID,
			Ordinal:        relationOrdinal(index),
			Description:    description,
			Title:          title,
			URL:            url,
			Hash:           helper.JavaStringHash(hashSource),
			LastModifiedAt: observedAt,
		})
	}
	return items
}

func (r *XmlReleaseRelation) GetIdentifiers() []*opendiscogsmodel.ReleaseItemIdentifier {
	observedAt := r.timestamp()
	items := make([]*opendiscogsmodel.ReleaseItemIdentifier, 0, len(r.Identifiers))
	for index, identifier := range r.Identifiers {
		description := helper.FilterStr(&identifier.Description)
		identifierType := helper.FilterStr(&identifier.Type)
		value := helper.FilterStr(&identifier.Value)
		hashSource := stringValue(identifierType) + stringValue(description) + stringValue(value)
		if strings.TrimSpace(hashSource) == "" {
			continue
		}
		items = append(items, &opendiscogsmodel.ReleaseItemIdentifier{
			ReleaseItemID:  r.ID,
			Ordinal:        relationOrdinal(index),
			Description:    description,
			Type:           identifierType,
			Value:          value,
			Hash:           helper.JavaStringHash(hashSource),
			LastModifiedAt: observedAt,
		})
	}
	return items
}

func (r *XmlReleaseRelation) GetTracks() []*opendiscogsmodel.ReleaseItemTrack {
	observedAt := r.timestamp()
	items := make([]*opendiscogsmodel.ReleaseItemTrack, 0, len(r.Tracks))
	for index, track := range r.Tracks {
		duration := helper.FilterStr(&track.Duration)
		position := helper.FilterStr(&track.Position)
		title := helper.FilterStr(&track.Title)
		hashSource := stringValue(position) + stringValue(title) + stringValue(duration)
		if strings.TrimSpace(hashSource) == "" {
			continue
		}
		items = append(items, &opendiscogsmodel.ReleaseItemTrack{
			ReleaseItemID:  r.ID,
			Ordinal:        relationOrdinal(index),
			Duration:       duration,
			Position:       position,
			Title:          title,
			Hash:           helper.JavaStringHash(hashSource),
			LastModifiedAt: observedAt,
		})
	}
	return items
}

func (r *XmlReleaseRelation) GetFormats() []*opendiscogsmodel.ReleaseItemFormat {
	observedAt := r.timestamp()
	items := make([]*opendiscogsmodel.ReleaseItemFormat, 0, len(r.Formats))
	for index, format := range r.Formats {
		description := reducedDescription(format.Descriptions)
		name := helper.FilterStr(format.Name)
		text := helper.FilterStr(format.Text)
		if name == nil && description == nil && text == nil {
			continue
		}
		quantity := format.Quantity.Integer()
		quantityText := format.Quantity.Canonical()
		hashSource := releaseFormatHashSource(name, description, quantityText, text)
		items = append(items, &opendiscogsmodel.ReleaseItemFormat{
			ReleaseItemID:  r.ID,
			Ordinal:        relationOrdinal(index),
			Description:    description,
			Name:           name,
			Quantity:       quantity,
			QuantityText:   quantityText,
			Text:           text,
			Hash:           helper.JavaStringHash(hashSource),
			LastModifiedAt: observedAt,
		})
	}
	return items
}

func releaseFormatHashSource(name, description, quantity, text *string) string {
	quantityValue := releaseFormatHashNullValue
	if quantity != nil {
		quantityValue = *quantity
	}
	return strings.Join([]string{
		releaseFormatHashString(name),
		releaseFormatHashString(description),
		quantityValue,
		releaseFormatHashString(text),
	}, releaseFormatHashFieldSeparator)
}

func releaseFormatHashString(value *string) string {
	if value == nil {
		return releaseFormatHashNullValue
	}
	return *value
}

func (r *XmlReleaseRelation) GetCreditedArtists() []*opendiscogsmodel.ReleaseItemCreditedArtist {
	observedAt := r.timestamp()
	items := make([]*opendiscogsmodel.ReleaseItemCreditedArtist, 0, len(r.CreditedArtists))
	for index, creditedArtist := range r.CreditedArtists {
		if !cache.ArtistIDs.Contains(creditedArtist.ArtistID) {
			continue
		}
		role := strings.TrimSpace(creditedArtist.Role)
		if role == "" {
			continue
		}
		items = append(items, &opendiscogsmodel.ReleaseItemCreditedArtist{
			ReleaseItemID:  r.ID,
			ArtistID:       creditedArtist.ArtistID,
			Ordinal:        relationOrdinal(index),
			Role:           &role,
			Hash:           helper.JavaStringHash(role),
			LastModifiedAt: observedAt,
		})
	}
	return items
}

func (r *XmlReleaseRelation) GetReleaseArtists() []*opendiscogsmodel.ReleaseItemArtist {
	observedAt := r.timestamp()
	items := make([]*opendiscogsmodel.ReleaseItemArtist, 0, len(r.Artists))
	for index, artistID := range r.Artists {
		if !cache.ArtistIDs.Contains(artistID) {
			continue
		}
		items = append(items, &opendiscogsmodel.ReleaseItemArtist{
			ReleaseItemID:  r.ID,
			ArtistID:       artistID,
			Ordinal:        relationOrdinal(index),
			LastModifiedAt: observedAt,
		})
	}
	return items
}

func (r *XmlReleaseRelation) GetLabels() []*opendiscogsmodel.LabelReleaseItem {
	observedAt := r.timestamp()
	items := make([]*opendiscogsmodel.LabelReleaseItem, 0, len(r.Labels))
	type identity struct {
		labelID          int32
		categoryNotation string
	}
	seen := make(map[identity]struct{}, len(r.Labels))
	for index, label := range r.Labels {
		if !cache.LabelIDs.Contains(label.LabelID) {
			continue
		}
		categoryNotation := strings.TrimSpace(label.CategoryNotation)
		key := identity{labelID: label.LabelID, categoryNotation: categoryNotation}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		items = append(items, &opendiscogsmodel.LabelReleaseItem{
			LabelID:          label.LabelID,
			ReleaseItemID:    r.ID,
			Ordinal:          relationOrdinal(index),
			CategoryNotation: helper.FilterStr(&categoryNotation),
			LastModifiedAt:   observedAt,
		})
	}
	return items
}

func (r *XmlReleaseRelation) GetReleaseStyles() []*opendiscogsmodel.ReleaseItemStyle {
	observedAt := r.timestamp()
	items := make([]*opendiscogsmodel.ReleaseItemStyle, 0, len(r.Styles))
	seen := make(map[string]struct{}, len(r.Styles))
	for index, value := range r.Styles {
		style := strings.TrimSpace(value)
		if style == "" {
			continue
		}
		if _, exists := seen[style]; exists {
			continue
		}
		seen[style] = struct{}{}
		items = append(items, &opendiscogsmodel.ReleaseItemStyle{
			ReleaseItemID:  r.ID,
			Ordinal:        relationOrdinal(index),
			Style:          style,
			LastModifiedAt: observedAt,
		})
	}
	return items
}

func (r *XmlReleaseRelation) GetReleaseGenres() []*opendiscogsmodel.ReleaseItemGenre {
	observedAt := r.timestamp()
	items := make([]*opendiscogsmodel.ReleaseItemGenre, 0, len(r.Genres))
	seen := make(map[string]struct{}, len(r.Genres))
	for index, value := range r.Genres {
		genre := strings.TrimSpace(value)
		if genre == "" {
			continue
		}
		if _, exists := seen[genre]; exists {
			continue
		}
		seen[genre] = struct{}{}
		items = append(items, &opendiscogsmodel.ReleaseItemGenre{
			ReleaseItemID:  r.ID,
			Ordinal:        relationOrdinal(index),
			Genre:          genre,
			LastModifiedAt: observedAt,
		})
	}
	return items
}

func releaseItem(
	id int32,
	title *string,
	country *string,
	dataQuality *string,
	listedReleaseDate *string,
	notes *string,
	masterInfo XmlReleaseMasterInfo,
	status *string,
	observedAt time.Time,
) *opendiscogsmodel.ReleaseItem {
	var masterID *int32
	if masterInfo.MasterID != nil {
		if cache.MasterIDs.Contains(*masterInfo.MasterID) {
			masterID = masterInfo.MasterID
		}
	}
	releaseDate, validYear, validMonth, validDay := parsedReleaseDate(listedReleaseDate)
	return &opendiscogsmodel.ReleaseItem{
		ID:                id,
		CreatedAt:         observedAt,
		LastModifiedAt:    observedAt,
		Title:             helper.FilterStr(title),
		Country:           helper.FilterStr(country),
		DataQuality:       helper.FilterStr(dataQuality),
		ReleaseDate:       releaseDate,
		HasValidYear:      &validYear,
		HasValidMonth:     &validMonth,
		HasValidDay:       &validDay,
		ListedReleaseDate: helper.FilterStr(listedReleaseDate),
		IsMaster:          &masterInfo.IsMaster,
		MasterID:          masterID,
		Notes:             helper.FilterStr(notes),
		Status:            helper.FilterStr(status),
	}
}

func parsedReleaseDate(value *string) (*time.Time, bool, bool, bool) {
	if value == nil {
		return nil, false, false, false
	}
	year, month, day := dateparser.ParseYMD(*value)
	if year == nil {
		return nil, false, false, false
	}
	monthValue := int16(1)
	dayValue := int16(1)
	if month != nil {
		monthValue = *month
	}
	if day != nil {
		dayValue = *day
	}
	parsed := time.Date(
		int(*year),
		time.Month(monthValue),
		int(dayValue),
		0,
		0,
		0,
		0,
		time.UTC,
	)
	return &parsed, true, month != nil, day != nil
}

func genres(values []string) []*opendiscogsmodel.Genre {
	items := make([]*opendiscogsmodel.Genre, 0, len(values))
	for _, value := range deduplicateComparable(values) {
		name := strings.TrimSpace(value)
		if name != "" {
			items = append(items, &opendiscogsmodel.Genre{Name: name})
		}
	}
	return items
}

func styles(values []string) []*opendiscogsmodel.Style {
	items := make([]*opendiscogsmodel.Style, 0, len(values))
	for _, value := range deduplicateComparable(values) {
		name := strings.TrimSpace(value)
		if name != "" {
			items = append(items, &opendiscogsmodel.Style{Name: name})
		}
	}
	return items
}

func reducedDescription(values []string) *string {
	descriptions := make([]string, 0, len(values))
	for _, value := range values {
		description := strings.TrimSpace(value)
		if description != "" {
			descriptions = append(descriptions, "[d:"+description+"]")
		}
	}
	if len(descriptions) == 0 {
		return nil
	}
	reduced := strings.Join(descriptions, ",")
	return &reduced
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
