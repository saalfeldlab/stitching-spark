package org.janelia.flatfield;

import java.util.Arrays;

import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileLoader;

import net.imglib2.Cursor;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.histogram.Real1dBinMapper;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.util.Pair;
import net.imglib2.util.ValuePair;
import net.imglib2.view.Views;

public class StackHistogram
{
	private final long[] stackHistogram;
	private final HistogramSettings stackHistogramSettings;

	private StackHistogram( final long[] stackHistogram, final HistogramSettings stackHistogramSettings )
	{
		this.stackHistogram = stackHistogram;
		this.stackHistogramSettings = stackHistogramSettings;
	}

	public Pair< Double, Double > getIntensityRange( final Pair< Double, Double > minMaxQuantiles )
	{
		final Real1dBinMapper< DoubleType > binMapper = new Real1dBinMapper<>( stackHistogramSettings.histMinValue, stackHistogramSettings.histMaxValue, stackHistogramSettings.bins, true );
		final long totalValuesCount = Arrays.stream( stackHistogram ).sum();
		long processedValuesCount = 0;
		final double[] minMaxQuantilesValues = new double[] { minMaxQuantiles.getA(), minMaxQuantiles.getB() };
		final Double[] minMaxRangeValues = new Double[ 2 ];
		for ( int bin = 0; bin < stackHistogramSettings.bins; ++bin )
		{
			processedValuesCount += stackHistogram[ bin ];
			for ( int i = 0; i < 2; ++i )
			{
				if ( minMaxRangeValues[ i ] == null && processedValuesCount >= Math.round( totalValuesCount * minMaxQuantilesValues[ i ] ) )
				{
					final DoubleType centerBinValue = new DoubleType();
					binMapper.getCenterValue( bin, centerBinValue );
					minMaxRangeValues[ i ] = centerBinValue.get();
				}
			}
		}
		if ( minMaxRangeValues[ 0 ] == null || minMaxRangeValues[ 1 ] == null )
			throw new RuntimeException( "min/max range cannot be estimated" );
		return new ValuePair<>( minMaxRangeValues[ 0 ], minMaxRangeValues[ 1 ] );
	}

	public double getPivotValue()
	{
		// simply use the most frequent value as the pivot value (this typically represents background)
		int mostFrequentValueBin = 0;
		for ( int bin = 0; bin < stackHistogramSettings.bins; ++bin )
			if ( stackHistogram[ bin ] > stackHistogram[ mostFrequentValueBin ] )
				mostFrequentValueBin = bin;

		final Real1dBinMapper< DoubleType > binMapper = new Real1dBinMapper<>( stackHistogramSettings.histMinValue, stackHistogramSettings.histMaxValue, stackHistogramSettings.bins, true );
		final DoubleType centerBinValue = new DoubleType();
		binMapper.getCenterValue( mostFrequentValueBin, centerBinValue );
		return centerBinValue.get();
	}

	public static < T extends NativeType< T > & RealType< T > > StackHistogram getStackHistogram(
			final JavaSparkContext sparkContext,
			final TileInfo[] tiles )
	{
		final HistogramSettings stackHistogramSettings = new HistogramSettings( 0., 16383., 4098 );
		final long[] stackHistogram = sparkContext.parallelize( Arrays.asList( tiles ), tiles.length ).map( tile ->
			{
				final long[] histogram = new long[ stackHistogramSettings.bins ];
				final Real1dBinMapper< T > binMapper = new Real1dBinMapper<>( stackHistogramSettings.histMinValue, stackHistogramSettings.histMaxValue, stackHistogramSettings.bins, true );
				final RandomAccessibleInterval< T > tileImg = TileLoader.loadTile( tile, DataProviderFactory.create( DataProviderFactory.detectType( tile.getFilePath() ) ) );
				final Cursor< T > cursor = Views.iterable( tileImg ).cursor();
				while ( cursor.hasNext() )
					++histogram[ ( int ) binMapper.map( cursor.next() ) ];
				return histogram;
			}
		)
		.treeReduce( ( histogram, other ) ->
			{
				for ( int bin = 0; bin < stackHistogramSettings.bins; ++bin )
					histogram[ bin ] += other[ bin ];
				return histogram;
			},
			Integer.MAX_VALUE // max possible aggregation depth
		);

		return new StackHistogram( stackHistogram, stackHistogramSettings );
	}
}
