package org.janelia.flatfield;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.bdv.DataAccessType;
import org.janelia.saalfeldlab.n5.spark.N5RemoveSpark;
import org.janelia.saalfeldlab.n5.spark.downsample.scalepyramid.N5OffsetScalePyramidSpark;

import bdv.export.Downsample;
import net.imglib2.FinalInterval;
import net.imglib2.Interval;
import net.imglib2.RandomAccessible;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.RealRandomAccessible;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.AffineGet;
import net.imglib2.realtransform.AffineSet;
import net.imglib2.realtransform.AffineTransform;
import net.imglib2.realtransform.AffineTransform2D;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.realtransform.RealViews;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Intervals;
import net.imglib2.util.Util;
import net.imglib2.view.ExtendedRandomAccessibleInterval;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

public class ShiftedDownsampling< A extends AffineGet & AffineSet >
{
	private transient final JavaSparkContext sparkContext;
	private final String downsampledHistogramsGroupPath;
	private final A downsamplingTransform;
	private final List< String > scalePyramidDatasetPaths;
	private final List< long[] > scalePyramidDatasetDimensions;

	private final DataAccessType dataAccessType;
	private final String histogramsN5BasePath;

	public ShiftedDownsampling( final JavaSparkContext sparkContext, final HistogramsProvider histogramsProvider ) throws IOException
	{
		this(
				sparkContext,
				histogramsProvider.getDataAccessType(),
				histogramsProvider.getHistogramsN5BasePath(),
				histogramsProvider.getHistogramsDataset(),
				histogramsProvider.getWorkingInterval()
			);
	}

	@SuppressWarnings( "unchecked" )
	public ShiftedDownsampling(
			final JavaSparkContext sparkContext,
			final DataAccessType dataAccessType,
			final String histogramsN5BasePath,
			final String fullScaleHistogramsDataset,
			final Interval workingInterval ) throws IOException
	{
		this.sparkContext = sparkContext;
		this.dataAccessType = dataAccessType;
		this.histogramsN5BasePath = histogramsN5BasePath;

		if ( workingInterval.numDimensions() == 2 )
			downsamplingTransform = ( A ) new AffineTransform2D();
		else if ( workingInterval.numDimensions() == 3 )
			downsamplingTransform = ( A ) new AffineTransform3D();
		else
			downsamplingTransform = ( A ) new AffineTransform( workingInterval.numDimensions() );
		for ( int d = 0; d < downsamplingTransform.numDimensions(); ++d )
		{
			downsamplingTransform.set( 0.5, d, d );
			downsamplingTransform.set( -0.5, d, downsamplingTransform.numDimensions() );
		}

		final DataProvider dataProvider = DataProviderFactory.createByType( dataAccessType );

		final String histogramsDatasetParentGroupPath = ( Paths.get( fullScaleHistogramsDataset ).getParent() != null ? Paths.get( fullScaleHistogramsDataset ).getParent().toString() : "" );
		downsampledHistogramsGroupPath = Paths.get( histogramsDatasetParentGroupPath, "histograms-downsampled" ).toString();

		// last extra dimension is for bins and should not be downsampled
		final int[] downsamplingFactors = new int[ workingInterval.numDimensions() + 1 ];
		Arrays.fill( downsamplingFactors, 2 );
		downsamplingFactors[ downsamplingFactors.length - 1 ] = 1;

		// do not offset the extra 'bins' dimension
		final boolean[] dimensionsWithOffset = new boolean[ workingInterval.numDimensions() + 1 ];
		Arrays.fill( dimensionsWithOffset, true );
		dimensionsWithOffset[ dimensionsWithOffset.length - 1 ] = false;

		scalePyramidDatasetPaths = new ArrayList<>();
		scalePyramidDatasetPaths.add( fullScaleHistogramsDataset );
		scalePyramidDatasetPaths.addAll( N5OffsetScalePyramidSpark.downsampleOffsetScalePyramid(
				sparkContext,
				() -> DataProviderFactory.createByType( dataAccessType ).createN5Writer( URI.create( histogramsN5BasePath ) ),
				fullScaleHistogramsDataset,
				downsampledHistogramsGroupPath,
				downsamplingFactors,
				dimensionsWithOffset
			) );

		scalePyramidDatasetDimensions = new ArrayList<>();
		final N5Reader n5 = dataProvider.createN5Reader( URI.create( histogramsN5BasePath ) );
		for ( final String scalePyramidDatasetPath : scalePyramidDatasetPaths )
		{
			final long[] extendedDimensions = n5.getDatasetAttributes( scalePyramidDatasetPath ).getDimensions();
			final long[] dimensions = new long[ extendedDimensions.length - 1 ];
			System.arraycopy( extendedDimensions, 0, dimensions, 0, dimensions.length );
			scalePyramidDatasetDimensions.add( dimensions );
		}
	}

	public < T extends NativeType< T > & RealType< T > > RandomAccessibleInterval< T > downsampleImage(
			final RandomAccessibleInterval< T > fullComponent,
			final int scale )
	{
		if ( scale == 0 )
			return fullComponent;

		final long[] downsampledDimensions = scalePyramidDatasetDimensions.get( scale );
		final int[] factor = new int[ downsampledDimensions.length ];
		Arrays.fill( factor, 1 << scale );
		final long[] shift = new long[ downsampledDimensions.length ];
		Arrays.fill( shift, 1 << ( scale - 1 ) );

		final IntervalView< T > shiftedFullComponent = Views.translate( fullComponent, shift );
		final ExtendedRandomAccessibleInterval< T, RandomAccessibleInterval< T > > extendedShiftedFullComponent = Views.extendMirrorDouble( shiftedFullComponent );

		final T type = Util.getTypeFromInterval( fullComponent );
		final RandomAccessibleInterval< T > downsampledComponent = new ArrayImgFactory< T >().create( downsampledDimensions, type );

		Downsample.downsample( extendedShiftedFullComponent, downsampledComponent, factor );

		return downsampledComponent;
	}

	@SuppressWarnings( "unchecked" )
	public < T extends RealType< T > & NativeType< T > > RandomAccessible< T > upsampleImage(
			final RandomAccessibleInterval< T > downsampledImg,
			final int newScale )
	{
		// find the scale level of downsampledImg by comparing the dimensions
		int oldScale = -1;
		for ( int scale = scalePyramidDatasetPaths.size() - 1; scale >= 0; scale-- )
		{
			if ( Intervals.equalDimensions( downsampledImg, new FinalInterval( scalePyramidDatasetDimensions.get( scale ) ) ) )
			{
				oldScale = scale;
				break;
			}
		}
		if ( oldScale == -1 )
			throw new IllegalArgumentException( "Cannot identify scale level of the given image" );

		RealRandomAccessible< T > img = Views.interpolate( Views.extendBorder( downsampledImg ), new NLinearInterpolatorFactory<>() );
		for ( int scale = oldScale - 1; scale >= newScale; scale-- )
		{
			// Preapply the shifting transform in order to align the downsampled image to the upsampled image (in its coordinate space)
			final A translationTransform = ( A ) downsamplingTransform.copy();
			for ( int d = 0; d < translationTransform.numDimensions(); d++ )
			{
				translationTransform.set( 1, d, d );
				if ( scale == 0 )
					translationTransform.set( 1.5 * translationTransform.get(
							d, translationTransform.numDimensions() ),
							d, translationTransform.numDimensions() );
			}

			final RealRandomAccessible< T > alignedDownsampledImg = RealViews.affine( img, translationTransform );

			// Then, apply the inverse of the downsampling transform in order to map the downsampled image to the upsampled image
			img = RealViews.affine( alignedDownsampledImg, downsamplingTransform.inverse() );
		}
		return Views.raster( img );
	}

	public void cleanupDownsampledHistograms() throws IOException
	{
		final DataAccessType dataAccessType = this.dataAccessType;
		final String histogramsN5BasePath = this.histogramsN5BasePath;
		final String downsampledHistogramsGroupPath = this.downsampledHistogramsGroupPath;

		N5RemoveSpark.remove(
				sparkContext,
				() -> DataProviderFactory.createByType( dataAccessType ).createN5Writer( URI.create( histogramsN5BasePath ) ),
				downsampledHistogramsGroupPath
			);
	}

	public int getNumScales()
	{
		return scalePyramidDatasetDimensions.size();
	}

	public long[] getDimensionsAtScale( final int scale )
	{
		return scalePyramidDatasetDimensions.get( scale );
	}

	public String getDatasetAtScale( final int scale )
	{
		return scalePyramidDatasetPaths.get( scale );
	}
}
