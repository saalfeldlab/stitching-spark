package org.janelia.flatfield;

import java.util.Arrays;

import org.apache.commons.lang3.NotImplementedException;
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
	public static enum QuantileMode
	{
		LowerBound,
		UpperBound,
		CenterValue
	}

	private final long[] stackHistogram;
	private final HistogramSettings stackHistogramSettings;

	private StackHistogram( final long[] stackHistogram, final HistogramSettings stackHistogramSettings )
	{
		this.stackHistogram = stackHistogram;
		this.stackHistogramSettings = stackHistogramSettings;
	}

	public Pair< Double, Double > getIntensityRange()
	{
		return getIntensityRange( new ValuePair<>( 0., 1.) );
	}

	public Pair< Double, Double > getIntensityRange( final Pair< Double, Double > minMaxQuantiles )
	{
		return getIntensityRange( minMaxQuantiles, new ValuePair<>( QuantileMode.LowerBound, QuantileMode.UpperBound ) );
	}

	public Pair< Double, Double > getIntensityRange( final Pair< Double, Double > minMaxQuantiles, final Pair< QuantileMode, QuantileMode > minMaxQuantileModes )
	{
		return new ValuePair<>(
				getQuantile( minMaxQuantiles.getA(), minMaxQuantileModes.getA() ),
				getQuantile( minMaxQuantiles.getB(), minMaxQuantileModes.getB() )
			);
	}

	public double getQuantile( final double quantile, final QuantileMode quantileMode )
	{
		final Real1dBinMapper< DoubleType > binMapper = new Real1dBinMapper<>( stackHistogramSettings.histMinValue, stackHistogramSettings.histMaxValue, stackHistogramSettings.bins, true );
		final long totalValuesCount = Arrays.stream( stackHistogram ).sum();
		long processedValuesCount = 0;
		Double quantileValue = null;
		for ( int bin = 0; bin < stackHistogramSettings.bins; ++bin )
		{
			processedValuesCount += stackHistogram[ bin ];
			if ( processedValuesCount >= Math.round( totalValuesCount * quantile ) )
			{
				final DoubleType binValue = new DoubleType();
				switch ( quantileMode )
				{
				case LowerBound:
					binMapper.getLowerBound( bin, binValue );
					break;
				case UpperBound:
					binMapper.getUpperBound( bin, binValue );
					break;
				case CenterValue:
					binMapper.getCenterValue( bin, binValue );
					break;
				default:
					throw new NotImplementedException( "quantile mode " + quantileMode + " is not implemented yet" );
				}
				quantileValue = binValue.get();
				break;
			}
		}

		if ( quantileValue == null )
			throw new RuntimeException( "quantile cannot be estimated" );

		if ( Double.isNaN( quantileValue ) )
			throw new RuntimeException( "estimated quantile is NaN" );

		// prevent from going to infinity
		if ( Double.isInfinite( quantileValue ) )
			quantileValue = quantileValue < 0 ? stackHistogramSettings.histMinValue : stackHistogramSettings.histMaxValue;

		return quantileValue;
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
			final TileInfo[] tiles,
			final HistogramSettings stackHistogramSettings )
	{
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

	@Override
	public String toString()
	{
		final StringBuilder sb = new StringBuilder();
		sb.append( String.format( "[ min=%.2f, max=%.2f, bins=%d (including two tail bins) ]", stackHistogramSettings.histMinValue, stackHistogramSettings.histMaxValue, stackHistogramSettings.bins ) ).append( System.lineSeparator() );
		final Real1dBinMapper< DoubleType > binMapper = new Real1dBinMapper<>( stackHistogramSettings.histMinValue, stackHistogramSettings.histMaxValue, stackHistogramSettings.bins, true );
		final DoubleType lowerBinValue = new DoubleType(), upperBinValue = new DoubleType();
		for ( int bin = 0; bin < stackHistogramSettings.bins; ++bin )
		{
			binMapper.getLowerBound( bin, lowerBinValue );
			binMapper.getUpperBound( bin, upperBinValue );
			sb.append( String.format(
					"  %d: %d  [%.2f - %.2f]",
					bin,
					stackHistogram[ bin ],
					lowerBinValue.get(),
					upperBinValue.get()
				) )
			.append( System.lineSeparator() );
		}
		return sb.toString();
	}
}
