package batch

import (
	"encoding/xml"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dsub-io/go-open-discogs-batch/src/cache"
	"github.com/dsub-io/go-open-discogs-batch/src/dateparser"
	"github.com/dsub-io/go-open-discogs-batch/src/helper"
	opendiscogsmodel "github.com/dsub-io/open-discogs-model/model"
	"github.com/reactivex/rxgo/v2"
)

const (
	releaseFormatHashFieldSeparator = "\x00"
	releaseFormatHashNullValue      = "\x01"
)

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

func (a *XmlArtist) Transform() rxgo.Observable {
	now := time.Now().UTC()
	return rxgo.Just(&opendiscogsmodel.Artist{
		ID:             a.ID,
		CreatedAt:      now,
		LastModifiedAt: now,
		DataQuality:    helper.FilterStr(a.DataQuality),
		Name:           helper.FilterStr(a.Name),
		Profile:        helper.FilterStr(a.Profile),
		RealName:       helper.FilterStr(a.RealName),
	})()
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
}

func (a *XmlArtistRelation) GetArtist() *opendiscogsmodel.Artist {
	now := time.Now().UTC()
	return &opendiscogsmodel.Artist{
		ID:             a.ID,
		CreatedAt:      now,
		LastModifiedAt: now,
		DataQuality:    helper.FilterStr(a.DataQuality),
		Name:           helper.FilterStr(a.Name),
		Profile:        helper.FilterStr(a.Profile),
		RealName:       helper.FilterStr(a.RealName),
	}
}

func (a *XmlArtistRelation) GetUrls() []*opendiscogsmodel.ArtistURL {
	items := make([]*opendiscogsmodel.ArtistURL, 0, len(a.URLs))
	for _, value := range a.URLs {
		url := strings.TrimSpace(value)
		if url == "" {
			continue
		}
		now := time.Now().UTC()
		items = append(items, &opendiscogsmodel.ArtistURL{
			ArtistID:       a.ID,
			Hash:           helper.JavaStringHash(url),
			URL:            url,
			CreatedAt:      now,
			LastModifiedAt: now,
		})
	}
	return items
}

func (a *XmlArtistRelation) GetNameVars() []*opendiscogsmodel.ArtistNameVariation {
	items := make([]*opendiscogsmodel.ArtistNameVariation, 0, len(a.NameVars))
	for _, value := range a.NameVars {
		nameVariation := strings.TrimSpace(value)
		if nameVariation == "" {
			continue
		}
		now := time.Now().UTC()
		items = append(items, &opendiscogsmodel.ArtistNameVariation{
			ArtistID:       a.ID,
			NameVariation:  nameVariation,
			Hash:           helper.JavaStringHash(nameVariation),
			CreatedAt:      now,
			LastModifiedAt: now,
		})
	}
	return items
}

func (a *XmlArtistRelation) GetAliases() []*opendiscogsmodel.ArtistAlias {
	items := make([]*opendiscogsmodel.ArtistAlias, 0, len(a.Aliases))
	for _, alias := range a.Aliases {
		if !cache.ArtistIDs.Contains(alias.ID) {
			continue
		}
		now := time.Now().UTC()
		items = append(items, &opendiscogsmodel.ArtistAlias{
			ArtistID:       a.ID,
			AliasID:        alias.ID,
			CreatedAt:      now,
			LastModifiedAt: now,
		})
	}
	return items
}

func (a *XmlArtistRelation) GetGroups() []*opendiscogsmodel.ArtistGroup {
	items := make([]*opendiscogsmodel.ArtistGroup, 0, len(a.Groups))
	for _, group := range a.Groups {
		if !cache.ArtistIDs.Contains(group.ID) {
			continue
		}
		now := time.Now().UTC()
		items = append(items, &opendiscogsmodel.ArtistGroup{
			ArtistID:       a.ID,
			GroupID:        group.ID,
			CreatedAt:      now,
			LastModifiedAt: now,
		})
	}
	return items
}

func (a *XmlArtistRelation) GetMembers() []*opendiscogsmodel.ArtistMember {
	items := make([]*opendiscogsmodel.ArtistMember, 0, len(a.Members))
	for _, member := range a.Members {
		if !cache.ArtistIDs.Contains(member.ID) {
			continue
		}
		now := time.Now().UTC()
		items = append(items, &opendiscogsmodel.ArtistMember{
			ArtistID:       a.ID,
			MemberID:       member.ID,
			CreatedAt:      now,
			LastModifiedAt: now,
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

func (l *XmlLabel) Transform() rxgo.Observable {
	now := time.Now().UTC()
	return rxgo.Just(&opendiscogsmodel.Label{
		ID:             l.ID,
		CreatedAt:      now,
		LastModifiedAt: now,
		Name:           helper.FilterStr(l.Name),
		ContactInfo:    helper.FilterStr(l.ContactInfo),
		Profile:        helper.FilterStr(l.Profile),
		DataQuality:    helper.FilterStr(l.DataQuality),
	})()
}

type XmlLabelRelation struct {
	ID          int32    `xml:"id"`
	Name        *string  `xml:"name"`
	ContactInfo *string  `xml:"contactinfo"`
	Profile     *string  `xml:"profile"`
	DataQuality *string  `xml:"data_quality"`
	URLs        []string `xml:"urls>url"`
	SubLabels   []XmlRef `xml:"sublabels>label"`
}

func (l *XmlLabelRelation) GetLabel() *opendiscogsmodel.Label {
	now := time.Now().UTC()
	return &opendiscogsmodel.Label{
		ID:             l.ID,
		CreatedAt:      now,
		LastModifiedAt: now,
		Name:           helper.FilterStr(l.Name),
		ContactInfo:    helper.FilterStr(l.ContactInfo),
		Profile:        helper.FilterStr(l.Profile),
		DataQuality:    helper.FilterStr(l.DataQuality),
	}
}

func (l *XmlLabelRelation) GetUrls() []*opendiscogsmodel.LabelURL {
	items := make([]*opendiscogsmodel.LabelURL, 0, len(l.URLs))
	for _, value := range l.URLs {
		url := strings.TrimSpace(value)
		if url == "" {
			continue
		}
		now := time.Now().UTC()
		items = append(items, &opendiscogsmodel.LabelURL{
			LabelID:        l.ID,
			Hash:           helper.JavaStringHash(url),
			URL:            url,
			CreatedAt:      now,
			LastModifiedAt: now,
		})
	}
	return items
}

func (l *XmlLabelRelation) GetSubLabels() []*opendiscogsmodel.LabelSubLabel {
	items := make([]*opendiscogsmodel.LabelSubLabel, 0, len(l.SubLabels))
	for _, subLabel := range l.SubLabels {
		if !cache.LabelIDs.Contains(subLabel.ID) {
			continue
		}
		now := time.Now().UTC()
		items = append(items, &opendiscogsmodel.LabelSubLabel{
			ParentLabelID:  l.ID,
			SubLabelID:     subLabel.ID,
			CreatedAt:      now,
			LastModifiedAt: now,
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

func (m *XmlMaster) Transform() rxgo.Observable {
	now := time.Now().UTC()
	return rxgo.Just(&opendiscogsmodel.Master{
		ID:             m.ID,
		CreatedAt:      now,
		LastModifiedAt: now,
		Title:          helper.FilterStr(m.Title),
		DataQuality:    helper.FilterStr(m.DataQuality),
		Year:           m.Year,
	})()
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
	now := time.Now().UTC()
	return &opendiscogsmodel.Master{
		ID:             m.ID,
		CreatedAt:      now,
		LastModifiedAt: now,
		Title:          helper.FilterStr(m.Title),
		DataQuality:    helper.FilterStr(m.DataQuality),
		Year:           m.Year,
	}
}

func (m *XmlMasterRelation) GetMasterStyles() []*opendiscogsmodel.MasterStyle {
	items := make([]*opendiscogsmodel.MasterStyle, 0, len(m.Styles))
	for _, value := range m.Styles {
		style := strings.TrimSpace(value)
		if style == "" {
			continue
		}
		now := time.Now().UTC()
		items = append(items, &opendiscogsmodel.MasterStyle{
			MasterID:       m.ID,
			Style:          style,
			CreatedAt:      now,
			LastModifiedAt: now,
		})
	}
	return items
}

func (m *XmlMasterRelation) GetMasterGenres() []*opendiscogsmodel.MasterGenre {
	items := make([]*opendiscogsmodel.MasterGenre, 0, len(m.Genres))
	for _, value := range m.Genres {
		genre := strings.TrimSpace(value)
		if genre == "" {
			continue
		}
		now := time.Now().UTC()
		items = append(items, &opendiscogsmodel.MasterGenre{
			MasterID:       m.ID,
			Genre:          genre,
			CreatedAt:      now,
			LastModifiedAt: now,
		})
	}
	return items
}

func (m *XmlMasterRelation) GetMasterVideos() []*opendiscogsmodel.MasterVideo {
	items := make([]*opendiscogsmodel.MasterVideo, 0, len(m.Videos))
	for _, video := range m.Videos {
		title := helper.FilterStr(video.Title)
		description := helper.FilterStr(video.Description)
		url := helper.FilterStr(&video.URL)
		hashSource := stringValue(title) + stringValue(description) + stringValue(url)
		if strings.TrimSpace(hashSource) == "" {
			continue
		}
		now := time.Now().UTC()
		items = append(items, &opendiscogsmodel.MasterVideo{
			MasterID:       m.ID,
			Hash:           helper.JavaStringHash(hashSource),
			URL:            url,
			Description:    description,
			Title:          title,
			CreatedAt:      now,
			LastModifiedAt: now,
		})
	}
	return items
}

func (m *XmlMasterRelation) GetMasterArtists() []*opendiscogsmodel.MasterArtist {
	items := make([]*opendiscogsmodel.MasterArtist, 0, len(m.Artists))
	for _, artistID := range m.Artists {
		if !cache.ArtistIDs.Contains(artistID) {
			continue
		}
		now := time.Now().UTC()
		items = append(items, &opendiscogsmodel.MasterArtist{
			ArtistID:       artistID,
			MasterID:       m.ID,
			CreatedAt:      now,
			LastModifiedAt: now,
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

func (r *XmlRelease) Transform() rxgo.Observable {
	return rxgo.Just(releaseItem(
		r.ID,
		r.Title,
		r.Country,
		r.DataQuality,
		r.ListedReleaseDate,
		r.Notes,
		r.MasterInfo,
		r.Status,
	))()
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
}

func (r *XmlReleaseRelation) GetGenres() []*opendiscogsmodel.Genre {
	return genres(r.Genres)
}

func (r *XmlReleaseRelation) GetStyles() []*opendiscogsmodel.Style {
	return styles(r.Styles)
}

func (r *XmlReleaseRelation) GetRelease() *opendiscogsmodel.ReleaseItem {
	return releaseItem(
		r.ID,
		r.Title,
		r.Country,
		r.DataQuality,
		r.ListedReleaseDate,
		r.Notes,
		r.MasterInfo,
		r.Status,
	)
}

func (r *XmlReleaseRelation) GetWorks() []*opendiscogsmodel.ReleaseItemWork {
	items := make([]*opendiscogsmodel.ReleaseItemWork, 0, len(r.Works))
	for _, work := range r.Works {
		value := strings.TrimSpace(work.Work)
		if value == "" {
			continue
		}
		if !cache.LabelIDs.Contains(work.LabelID) {
			continue
		}
		now := time.Now().UTC()
		items = append(items, &opendiscogsmodel.ReleaseItemWork{
			ReleaseItemID:  r.ID,
			LabelID:        work.LabelID,
			Work:           &value,
			Hash:           helper.JavaStringHash(value),
			CreatedAt:      now,
			LastModifiedAt: now,
		})
	}
	return items
}

func (r *XmlReleaseRelation) GetVideos() []*opendiscogsmodel.ReleaseItemVideo {
	items := make([]*opendiscogsmodel.ReleaseItemVideo, 0, len(r.Videos))
	for _, video := range r.Videos {
		title := helper.FilterStr(video.Title)
		description := helper.FilterStr(video.Description)
		url := helper.FilterStr(&video.URL)
		hashSource := stringValue(title) + stringValue(description) + stringValue(url)
		if strings.TrimSpace(hashSource) == "" {
			continue
		}
		now := time.Now().UTC()
		items = append(items, &opendiscogsmodel.ReleaseItemVideo{
			ReleaseItemID:  r.ID,
			Description:    description,
			Title:          title,
			URL:            url,
			Hash:           helper.JavaStringHash(hashSource),
			CreatedAt:      now,
			LastModifiedAt: now,
		})
	}
	return items
}

func (r *XmlReleaseRelation) GetIdentifiers() []*opendiscogsmodel.ReleaseItemIdentifier {
	items := make([]*opendiscogsmodel.ReleaseItemIdentifier, 0, len(r.Identifiers))
	for _, identifier := range r.Identifiers {
		description := helper.FilterStr(&identifier.Description)
		identifierType := helper.FilterStr(&identifier.Type)
		value := helper.FilterStr(&identifier.Value)
		hashSource := stringValue(identifierType) + stringValue(description) + stringValue(value)
		if strings.TrimSpace(hashSource) == "" {
			continue
		}
		now := time.Now().UTC()
		items = append(items, &opendiscogsmodel.ReleaseItemIdentifier{
			ReleaseItemID:  r.ID,
			Description:    description,
			Type:           identifierType,
			Value:          value,
			Hash:           helper.JavaStringHash(hashSource),
			CreatedAt:      now,
			LastModifiedAt: now,
		})
	}
	return items
}

func (r *XmlReleaseRelation) GetTracks() []*opendiscogsmodel.ReleaseItemTrack {
	items := make([]*opendiscogsmodel.ReleaseItemTrack, 0, len(r.Tracks))
	for _, track := range r.Tracks {
		duration := helper.FilterStr(&track.Duration)
		position := helper.FilterStr(&track.Position)
		title := helper.FilterStr(&track.Title)
		hashSource := stringValue(position) + stringValue(title) + stringValue(duration)
		if strings.TrimSpace(hashSource) == "" {
			continue
		}
		now := time.Now().UTC()
		items = append(items, &opendiscogsmodel.ReleaseItemTrack{
			ReleaseItemID:  r.ID,
			Duration:       duration,
			Position:       position,
			Title:          title,
			Hash:           helper.JavaStringHash(hashSource),
			CreatedAt:      now,
			LastModifiedAt: now,
		})
	}
	return items
}

func (r *XmlReleaseRelation) GetFormats() []*opendiscogsmodel.ReleaseItemFormat {
	items := make([]*opendiscogsmodel.ReleaseItemFormat, 0, len(r.Formats))
	for _, format := range r.Formats {
		description := reducedDescription(format.Descriptions)
		name := helper.FilterStr(format.Name)
		text := helper.FilterStr(format.Text)
		if name == nil && description == nil && text == nil {
			continue
		}
		quantity := format.Quantity.Integer()
		quantityText := format.Quantity.Canonical()
		hashSource := releaseFormatHashSource(name, description, quantityText, text)
		now := time.Now().UTC()
		items = append(items, &opendiscogsmodel.ReleaseItemFormat{
			ReleaseItemID:  r.ID,
			Description:    description,
			Name:           name,
			Quantity:       quantity,
			QuantityText:   quantityText,
			Text:           text,
			Hash:           helper.JavaStringHash(hashSource),
			CreatedAt:      now,
			LastModifiedAt: now,
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
	items := make([]*opendiscogsmodel.ReleaseItemCreditedArtist, 0, len(r.CreditedArtists))
	for _, creditedArtist := range r.CreditedArtists {
		if !cache.ArtistIDs.Contains(creditedArtist.ArtistID) {
			continue
		}
		role := strings.TrimSpace(creditedArtist.Role)
		if role == "" {
			continue
		}
		now := time.Now().UTC()
		items = append(items, &opendiscogsmodel.ReleaseItemCreditedArtist{
			ReleaseItemID:  r.ID,
			ArtistID:       creditedArtist.ArtistID,
			Role:           &role,
			Hash:           helper.JavaStringHash(role),
			CreatedAt:      now,
			LastModifiedAt: now,
		})
	}
	return items
}

func (r *XmlReleaseRelation) GetReleaseArtists() []*opendiscogsmodel.ReleaseItemArtist {
	items := make([]*opendiscogsmodel.ReleaseItemArtist, 0, len(r.Artists))
	for _, artistID := range r.Artists {
		if !cache.ArtistIDs.Contains(artistID) {
			continue
		}
		now := time.Now().UTC()
		items = append(items, &opendiscogsmodel.ReleaseItemArtist{
			ReleaseItemID:  r.ID,
			ArtistID:       artistID,
			CreatedAt:      now,
			LastModifiedAt: now,
		})
	}
	return items
}

func (r *XmlReleaseRelation) GetLabels() []*opendiscogsmodel.LabelReleaseItem {
	items := make([]*opendiscogsmodel.LabelReleaseItem, 0, len(r.Labels))
	type identity struct {
		labelID          int32
		categoryNotation string
	}
	seen := make(map[identity]struct{}, len(r.Labels))
	for _, label := range r.Labels {
		if !cache.LabelIDs.Contains(label.LabelID) {
			continue
		}
		categoryNotation := strings.TrimSpace(label.CategoryNotation)
		key := identity{labelID: label.LabelID, categoryNotation: categoryNotation}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		now := time.Now().UTC()
		items = append(items, &opendiscogsmodel.LabelReleaseItem{
			LabelID:          label.LabelID,
			ReleaseItemID:    r.ID,
			CategoryNotation: helper.FilterStr(&categoryNotation),
			CreatedAt:        now,
			LastModifiedAt:   now,
		})
	}
	return items
}

func (r *XmlReleaseRelation) GetReleaseStyles() []*opendiscogsmodel.ReleaseItemStyle {
	items := make([]*opendiscogsmodel.ReleaseItemStyle, 0, len(r.Styles))
	seen := make(map[string]struct{}, len(r.Styles))
	for _, value := range r.Styles {
		style := strings.TrimSpace(value)
		if style == "" {
			continue
		}
		if _, exists := seen[style]; exists {
			continue
		}
		seen[style] = struct{}{}
		now := time.Now().UTC()
		items = append(items, &opendiscogsmodel.ReleaseItemStyle{
			ReleaseItemID:  r.ID,
			Style:          style,
			CreatedAt:      now,
			LastModifiedAt: now,
		})
	}
	return items
}

func (r *XmlReleaseRelation) GetReleaseGenres() []*opendiscogsmodel.ReleaseItemGenre {
	items := make([]*opendiscogsmodel.ReleaseItemGenre, 0, len(r.Genres))
	seen := make(map[string]struct{}, len(r.Genres))
	for _, value := range r.Genres {
		genre := strings.TrimSpace(value)
		if genre == "" {
			continue
		}
		if _, exists := seen[genre]; exists {
			continue
		}
		seen[genre] = struct{}{}
		now := time.Now().UTC()
		items = append(items, &opendiscogsmodel.ReleaseItemGenre{
			ReleaseItemID:  r.ID,
			Genre:          genre,
			CreatedAt:      now,
			LastModifiedAt: now,
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
) *opendiscogsmodel.ReleaseItem {
	var masterID *int32
	if masterInfo.MasterID != nil {
		if cache.MasterIDs.Contains(*masterInfo.MasterID) {
			masterID = masterInfo.MasterID
		}
	}
	releaseDate, validYear, validMonth, validDay := parsedReleaseDate(listedReleaseDate)
	now := time.Now().UTC()
	return &opendiscogsmodel.ReleaseItem{
		ID:                id,
		CreatedAt:         now,
		LastModifiedAt:    now,
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
