package fgm.sketch;

import fgm.datatypes.VectorType;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.simple.RandomSource;

import java.util.Arrays;

public class AGMS extends VectorType {

    // External hash function used on project is xxh3 hash function for generating the key
    // Internal hash function used from this class is Murmur3 hash function
    // For functions estimate,update with argument String key will be used Murmur3 hash function to hashing the key

    public final static Long[][] seedVector = initializeHashFamilies();

    // These implementation is the same as the Fast AGMS sketch
    private double[][] sketchVector;

    // Constructors
    public AGMS(){}
    public AGMS(int depth, int width){
        sketchVector = new double[depth][width];
        for(double[] row : sketchVector) Arrays.fill(row, 0d);
    }
    public AGMS(double epsilon, double delta){

        // Calculate the depth
        int width = (int) (1.0d/Math.pow(epsilon,2));

        // Calculate width
        int depth = (int) Math.log(1/delta);

        // Initialization
        sketchVector = new double[depth][width];
        for(double[] row : sketchVector) Arrays.fill(row, 0d);
    }

    // Getters
    public double[][] getSketchVector() { return sketchVector; }

    // Setters
    public void setSketchVector(double[][] sketchVector) { this.sketchVector = sketchVector; }


    // Methods

    public int depth() { return sketchVector.length; }
    public int width() { return sketchVector[0].length; }
    public double epsilon(){ return 1.0d/Math.sqrt(this.width()); }
    public double delta(){ return Math.pow(2.0d,-this.depth()); }
    public int size() { return depth()*width(); }
    public int sizeBytes() { return depth()*width()*Long.BYTES; }

    // Hash mod 2^31
    private long hash31(long a, long b, long x) {
        long result = (a * x) + b;
        return ((result >> 31) ^ result) & 2147483647;
    }

    private int hash(int d, long x) {
        assert d < this.depth();
        return (int) hash31(seedVector[0][d], seedVector[1][d], x) % this.width();
    }

    // Four-wise independent
    private long fourWise(long s1, long s2, long s3, long s4, long x) {
        return hash31(hash31(hash31(x, s1, s2), x, s3), x, s4) & (1 << 15);
    }

    // Return a 4-wise independent number {-1,+1}
    private int fourWise(int d, long x) {
        return (fourWise(seedVector[2][d], seedVector[3][d], seedVector[4][d], seedVector[5][d], x) > 0) ? 1 : -1;
    }

    // Update the sketch
    public void update(long key, double value) {
        for(int d = 0; d < depth(); d++){

            int hash = hash(d, key); // Hash the key
            int xi = fourWise(d, key); // Second hash function

            // if( sketchVector[d][hash] == 0 && xi == -1 ) return;
            sketchVector[d][hash] += value*xi;
        }
    }

    // Estimate the count
    public double estimateAGMS(long key){

        double[] estimates = new double[depth()];

        for(int d = 0; d < depth(); d++){

            int hash = hash(d,key);
            int xi = fourWise(d,key);

            estimates[d] = sketchVector[d][hash]*xi;
        }
        // Get the median
        return median(estimates);
    }

    // Find the median from array estimates
    public double median(double[] estimates){

        // Corner cases
        if( estimates == null ) return 0;

        // Sort the array
        Arrays.sort(estimates);

        // Get the median
        if( estimates.length == 1) return estimates[0];
        else if( estimates.length == 2) return (estimates[0]+estimates[1])/2;
        else{
            // Get the length
            int length = estimates.length;

            if( length % 2 == 1) return estimates[(length+1)/2 - 1]; // Odd
            else return (estimates[length/2-1] + estimates[length/2]) / 2; // Even
        }
    }

    public double elementAt(int i, int j) { return sketchVector[i][j]; }

    public double[][] values() { return sketchVector; }

    public double[] getSketchColumn(int col) {
        double[] ret = new double[this.depth()];
        for(int d = 0; d < this.depth(); d++) ret[d] = sketchVector[d][col];
        return ret;
    }

    public void setSketchColumn(int col, double[] column) {
        for(int d = 0; d < this.depth(); d++) this.sketchVector[d][col] = column[d];
    }

    @Override
    public String toString() { return Arrays.deepToString(this.sketchVector)
                                      + " , depth : " + this.depth()
                                      + " , width : " + this.width()
                                      + " , size : " + this.size()
                                      + " , size in bytes : " + this.sizeBytes();
    }

    /**
     * From apache.commons javaDoc
     * Source of randomness is MersenneTwister , a pseudo-random generator of numbers
     * Native seed type: int[].
     * Native seed size: 624.
     */
    private static Long[][] initializeHashFamilies() {

        int max_sketch_depth = 11;
        int[] MT_seed = new int[]{1964674976, 1255696029, -126494180, 303310101, 1783995287, -956833270, -1044672147, -167460242, 217527981, 2046955694, 1387482168, -1840133342, -1978495461, -1715767084, -777465747, 668066454, 521076289, 1099760193, 1148696894, 473783256, 1261926014, 401420142, 1596274502, -1246980486, 55984283, 1621206817, 1675747910, 660558324, -885896623, 918395021, 1399625844, 1400617868, 402950124, -1035517327, -1840118528, 1324914184, 1433500137, 792088963, 1792337510, -949483604, -1019729240, -109948927, -1830596079, -1070126359, 642985452, 1313083847, 1746481448, 876459340, -398676735, 228790654, -1069750770, -1171201680, 725967876, -1637953948, 1641023169, -202692417, -513574558, -1798109195, -1454728780, -1761595644, 1879412294, 1155988384, 80128374, -1265379294, -1779758865, -224922974, 1089014564, 1788259966, -1543977595, -1736438100, -1210107495, 257849635, -285086751, -1509065371, 248785002, 1144823491, 79641015, -401747979, 536596005, 172724460, -1416836725, 2115971321, -1141395500, -1935622431, -1950568582, -1389705358, -492045524, 677508707, 327428112, 207720832, 8455517, -553465574, 1092222550, -64096556, -479774877, 1258163485, -1166996101, 1240884998, -76273021, -2139027871, 815586446, -886010335, 1373676716, -238034708, -344289237, -1621343975, 703681817, -399883955, -868989559, 2063627082, -402546694, 653645425, -1329379671, 604384442, 1309246910, 1886286741, -1026089562, 540732158, 967271646, 683356648, 1992400201, -97822462, 1090587504, 839739872, 1713255623, 1641160596, -551636328, -1413262449, 1409156127, 1941598891, -663483472, 170831887, -164475037, -129209209, -1877684134, 977270226, 775020283, -2062916754, 1693498567, -265125041, -2037874807, -1091826799, 1123448721, -1977899211, -1871471592, -993142488, -269892235, -2043539068, 333720812, -1092984443, 1802984050, -2025619940, 266447317, -648649900, -2087177130, 659561330, -582531224, 781071770, -167022134, 1710869998, -1898450932, -772271378, -98962166, 2003637743, 2077328423, -737391067, -126813834, 2099041926, -2115573016, -1048479518, 199292132, -172457132, 1143344977, -152678409, 2137638018, 2006117888, 822796865, -1759018848, 826518795, 132862953, 305778944, 1910581121, -176423785, 2020141907, 1102392150, 572932210, 191962133, 1990182545, 1890754221, 1515489049, 727506609, -174465455, -941322286, 1033323593, -1376561101, 259256100, -1747291342, -627035141, -900969958, 626210160, 1276428489, -1953891152, -1846308215, 945967362, -1617214870, 528983940, -373841764, 1966975379, -1953496464, 29287635, -821108033, 913330845, 425483854, -1267978331, 1274389145, -487245460, -911518431, 2116797231, 293872933, -658747248, 1244909748, -542442946, 1671686633, 2080078478, -1157267570, -743212575, -49470648, -181904166, 83940887, 1488989188, 135889415, 2142017086, -1735705817, 1480496104, 644513557, -1724781161, 242906171, -1483774082, 1540433000, 724195112, -1125060441, 1404168357, -1860113933, 1687888269, -1173023030, -263668071, 1652965640, -796038920, -2061844198, -1349775976, 990003852, -448720161, -912764743, -2086302277, 1974511214, -2010366854, -272396726, 1703311378, 873454381, 37778884, 958344286, -1120871392, -221028803, -1515852443, 566629928, 116090283, -508527973, -158217196, 244543580, -991643572, 1332791191, 1987382881, -1192857786, -761511603, -1538146431, -1693297609, 1750276028, -1878351202, -1194698440, -82316836, -1098384802, 1194398665, 95260438, -1812550478, 1791588520, 1568197682, -468441249, 822690819, -1988330278, 204698729, 1935765183, -658232870, -1231623358, -1892744070, 374845686, 1177566348, 1903037743, -1310501242, -1711915691, 629240171, -1635745063, -2106342900, 1864167653, 990941637, 104222342, 1645384892, 1458565030, -1814063643, 2072988646, 1070782180, -352840995, -1049717983, -484522735, -703491644, -308742189, 1417525924, -109827791, 2141516039, -1263765567, -187704300, 1555273885, 1151860773, -436105718, 1381005146, -187721752, -1212443656, -1275357082, -1443015491, -1302358397, 1889172411, 683573372, 833732699, 1441686233, 490183936, 934425450, 1519177704, -1340325015, -1449369144, 1560139980, -2132794405, 236480432, -284638226, -1607409758, 1771805490, -281651616, 819133481, 1332962653, 100743856, -507169675, -594747810, 151714358, 1227984141, 1017445527, -315059001, -244968369, 62468569, 1167842789, 1042434537, -1792242798, -707045366, 1860422913, 2064519408, 1961170614, 1276515334, -1715260969, 1271354889, -523781771, 489905640, 338245314, 1285324437, 847086680, 1848234016, 1436522672, 1985879287, 884029611, 1863597220, 1347143197, -829626954, 796764275, 653828163, 1739251137, 83035779, 1369600831, 1356504452, 756410048, 257335324, 1974961670, 2106379997, -1214600020, -1116692969, -753797886, 1662628691, -2064591281, 536234555, -1123147912, -1651592262, -975653960, -1833462890, -726166168, 1695125526, 413518434, 1016472988, -610741656, 1514862567, -522195041, -1707581980, 636811560, 600972546, 1755608819, -758240159, -1206477123, -877266086, -488388929, -171545977, -1022270557, -1347702034, 1803285344, 1260591778, -2103420909, -1052781514, -536300601, 1218415158, 856577079, 663252626, 617156788, -1621637809, 1509031530, 194918265, 505104601, 1748277593, 2134277580, 901614088, 908693052, -1390065928, 1932539929, -112211927, 1910032035, 2035439221, -1686154492, 935553954, -1868973461, 716120915, 1055895575, -1893609511, -597042090, -7586172, -1852554851, -1921075224, -1775713602, 1887117589, 1590993093, 342738072, -1149926132, 1407925528, -1818035069, -1855285220, 1834108096, 1979427683, -547723708, 1043378682, 1301238157, -1493660142, -39306898, 1848139851, -1202676587, 226847654, -40728198, 231251882, 759953229, -1609230602, -1015074284, -750174774, -85738331, 1558706499, 183969552, 1947259926, 598331018, -307690206, -869301295, 1025234272, -575690525, -68357065, 693915308, 1388247543, 300415800, 1870625788, 620498829, -1311266617, -944709600, -1996980395, -1298019420, -196218471, 1500522979, -748256303, 2039283107, -1549486214, 425061717, -1292258043, 2102207881, 1730543696, -226187897, -58440379, 630372536, -1508034992, 408470334, -314098281, 1219037081, -1101172120, -1633521839, -804708207, -1077416153, -1393960959, 52037108, -1601654276, -1802949691, 1158798599, 1915950004, -2559185, -384438486, -668700457, 1525995121, -1377153065, -2041743824, 973615738, -636662550, 1939874655, 1630007456, 949502162, -410490392, -2108202080, -1169936666, 692845427, -1082076, -569961750, -855497595, -496861017, 1170751428, -960177813, 139068587, 2088063885, -131151343, -727630460, 476810414, -211607018, 447302048, -1658069847, 1862031542, 1940339708, -704521268, 1620124956, 1497710148, -1832753567, -303784292, 1251342709, -577225167, 726621494, -1060774649, -1416127888, -1763747496, 808966067, 539896884, 1176809509, 1422756573, 823610680, 1522817110, -694365154, -988315261, -1236790305, 1217073111, 2120186694, -1757007894, -18184952, -1030914760, 1541812103, 1993399921, -1696296638, -1994391503, 123090148, -571280324, 1784028396, -1241078534, 1740118952, -1042618323, -784440085, 1376995925, -1878409423, -1012117756, 1487539801, -1058096044, 557049316, -1306604422, 717742585, 1802308979, 1131498767, -326057902, 1714426447, -451591590, -926198905, 1202407874, 2013577841, 698636409, 1702652301, 117158509, -558797829, 1712169853, -881160803, 485536770, -493144782, -1159089584, 255000867, 892855582, -1941676627, 250368379, -1542635631, 2101730245, 281098747, 42321670, -194900518, -1091443151, 563385722, -1264572286, -967857755, 766461572, -1039911786};
        UniformRandomProvider rng = RandomSource.create(RandomSource.MT, MT_seed);
        Long[][] seedVector = new Long[6][max_sketch_depth];

        for (int i = 0; i < 6; i++) {
            for (int j = 0; j < seedVector[0].length; j++) {
                long rand = rng.nextLong();
                seedVector[i][j] = (rand < 0) ? -rand : rand;
            }
        }
        return seedVector;
    }


    // Methods from abstract class VectorType
    // getKey and add method not used ???

    @Override
    public void update(String key){ this.update(Murmur3.hash64(key.getBytes()),1.0d); }

    @Override
    public void update(long hashedKey){ this.update(hashedKey,1.0d); }

    @Override
    public double estimate(String key){ return this.estimateAGMS(Murmur3.hash64(key.getBytes())); }

    @Override
    public double estimate(long hashedKey){ return this.estimateAGMS(hashedKey); }

    @Override
    public double[][] get(){ return this.getSketchVector(); }

    @Override
    public void set(double[][] sketch){ this.setSketchVector(sketch); }

    @Override
    public double[][] getEmptyInstance(){

        double[][] empty = new double[this.depth()][this.width()];
        for(double[] row : empty) Arrays.fill(row,0d);
        return empty;
    }

    @Override
    public void resetVector(){ for(double[] row : this.sketchVector) Arrays.fill(row,0d); }

    @Override
    public int getSize(){ return this.size(); }

    @Override
    public void addEntry(String key){}

    @Override
    public void addEntry(long hashedKey){}

    @Override
    public void getKey(){}

}
