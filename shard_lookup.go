package goshard

import (
	"math"
	"strconv"

	"github.com/pkg/errors"
)

// build an interface so the user can redefine ShardLookup
type ShardLookup interface {
	GetAllHosts() []string
	GetAll() []*ConnectionParams
	Lookup(uint64) (error, *ConnectionParams)
	GetShardId(uint64) uint
}

//build all the connection params
type RangeShardLookup struct {
	hosts []*ConnectionParams
}

//  RangeShardLookup implements ShardLookup
func NewShardLookup(hosts []*ConnectionParams) RangeShardLookup {
	return RangeShardLookup{hosts: hosts}
}

// return a list of hostnames
func (r RangeShardLookup) GetAllHosts() []string {
	var ret []string

	for _, cp := range r.hosts {

		ret = append(ret, cp.Host)

	}

	return ret
}

// return all the connection params
func (r RangeShardLookup) GetAll() []*ConnectionParams {
	return r.hosts
}

// for the shots version of shard
const ENTITIES_PER_SHARD uint64 = 200000

// skip a chunk of ids from old DC on the east cost to new DC on the west cost
const OLD_DC_UID = 3313360

// amount to skip
const USER_ID_GAP_ADJUSTMENT = 14193526

// don't let anyone else share special shards
const SKIP_JB_SHARD_ADJUSTMENT = 24193526

// return the shard id for a given entity_id
func (r RangeShardLookup) GetShardId(entity_id uint64) uint {

	var lookup_override map[uint64]uint = map[uint64]uint{
		215337:   51, //sirbizzle         741404
		2851622:  16, //shots             555518
		209799:   18, //ryanbutler        352498
		2205606:  21, //alfredoflores     345196
		2721589:  1,  //floydmayweather   312733
		17519275: 12, //kylizzlejennizle  240564
		2207508:  27, //jeremybieber      224519
		3012331:  19, //madisonbeer       221901
		2206037:  10, //djtayjames        213912
		2956708:  2,  //nickdemoura       173424
		3188639:  22, //khalil            163185
		222348:   27, //scooterbraun      154819
		17537932: 30, //snoopdogg         152662
		3058701:  30, //lilza             151097
		2932211:  28, //pattiemallette    148486
		2:        1,  //sammy             147729
		17549768: 10, //pizzle100         131332
		3163223:  27, //dankanter         129801
		2940269:  9,  //benjaminlasnier   127197
		209421:   13, //miketyson         121221
		17628444: 31, //amandasteele      119000
		3056390:  28, //daniellagrace     118663
		1:        3,  //johnny            113143
		2930347:  12, //erinwagner        111876
		17625290: 17, //kingbach          111773
		2930684:  31, //nickbilton        110927
		3213286:  16, //haileybaldwin     106060
		2959609:  7,  //maejorali         105716
		17607650: 9,  //selfiec           103426
		2938553:  10, //jaxonbieber       102774
		119824:   11, //ochocinco         102360
		17790657: 12, //redfoo             93261
		2938572:  17, //jazmynbieber       92252
		17661632: 22, //destorm            91884
		3178962:  15, //giovanna           90384
		2762678:  6,  //djirie             90187
		17530758: 16, //cairusso           78478
		17555239: 12, //stassiebaby        77269
		3174307:  15, //anthony            74301
		2951639:  12, //lukebroadlick      67896
		1330861:  10, //hoogs              65524
		3281319:  11, //christianbeadles   62869
		210453:   5,  //carin              62150
		17544401: 6,  //bizzlesbro_        60936
		2931080:  7,  //dustinfolkes       59008
		209670:   8,  //erinandrews        55898
		3008450:  9,  //maalikacosta       54921
		17522691: 10, //chanteezy          53744
		2972828:  1,  //jeppewest          52621
		3106450:  2,  //chazsomers         52373
		365228:   3,  //robkardashian      52343
		3241452:  4,  //helenowen          51989
		3161493:  5,  //jccaylen           51494
		2493355:  6,  //jozyaltidore       50985
		17721829: 27, //krisjenner         48988
		17915502: 13, //carter_reynolds    46039
		3117998:  10, //tashoakley         45688
		3281251:  1,  //devin_brugman      44614
		3198170:  2,  //tonyofficial       43367
		17583960: 9,  //souljaboy          42387
		3299551:  10, //willdanila         42378
		3081114:  11, //marianson_         41543
		2972003:  12, //yeshuathegudwin    41499
		17519272: 13, //jordynwoods        39739
		17578056: 14, //arianak            39617
		3241112:  15, //jakobkulke         38831
		2982818:  16, //rickybush_         38381
		3055925:  17, //jbforce            37675
		2931925:  18, //msalifiore         37287
		2930550:  19, //teddibiase         36037
		3306996:  20, //caitlinbeadles     34037
		17590094: 21, //trevormoran        33552
		2939732:  22, //sammisweetheart    33011
		17559611: 23, //kayden             32677
		2930305:  24, //tyflo              32281
		3185687:  25, //danielkucz         31764
		209302:   26, //crg                31102
		3075840:  27, //amazingkidjt       30948
		3298613:  28, //kylemassey         30850
		17903969: 29, //jleonlove          30692
		3121109:  30, //quincy             30362
		17682299: 31, //djvice             30230
		303629:   1,  //holdenvw           29579
		2931369:  2,  //badoujack          29471
		2933218:  3,  //jeffdarlington     28411
		304498:   4,  //vega               27939
		3227331:  5,  //hugo               27875
		2993834:  6,  //jessie             27686
		17574659: 7,  //yungrich           27506
		2931021:  8,  //byggis             27340
		3260856:  9,  //joeygreco          27219
		3073141:  10, //poobear            26982
		3065384:  11, //ramal              26920
		3098597:  12, //anastasiaashley    26689
		2479513:  13, //robinverrecas      26387
		17963673: 14, //garyvee            25456
		2362476:  15, //joseph             25028
		3189473:  16, //iaan               24832
		2948448:  17, //justincrew         24546
		17636888: 18, //vulpix             24446
	}

	if shard_id, ok := lookup_override[entity_id]; ok {
		return shard_id
	}

	if entity_id >= SKIP_JB_SHARD_ADJUSTMENT {
		entity_id += ENTITIES_PER_SHARD
	}

	if entity_id > OLD_DC_UID { // there is a big gap after 3313360
		entity_id = entity_id - USER_ID_GAP_ADJUSTMENT
	}
	floorit := (float64(entity_id) / float64(ENTITIES_PER_SHARD)) + 1.0
	shard_id := math.Floor((floorit))

	return uint(shard_id)
}

// lookup an entity
func (r RangeShardLookup) Lookup(entity_id uint64) (error, *ConnectionParams) {

	shard_id := r.GetShardId(entity_id)
	shard_id = shard_id - 1 // hosts is 0 based

	if r.hosts == nil || len(r.hosts) == 0 || shard_id > uint(len(r.hosts)) || r.hosts[shard_id] == nil {
		return errors.New("UNKNOWN SHARD_ID: " + strconv.Itoa(int(shard_id))), nil
	}

	return nil, r.hosts[shard_id]
}
