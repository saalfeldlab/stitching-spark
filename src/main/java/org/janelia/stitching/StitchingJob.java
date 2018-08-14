package org.janelia.stitching;

import java.io.File;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.saalfeldlab.n5.bdv.DataAccessType;

/**
 * Represents input parameters and customizations for tweaking the stitching/fusing procedure.
 *
 * @author Igor Pisarev
 */

public class StitchingJob implements Serializable {

	public enum PipelineStep
	{
		Metadata, // mandatory step
		Blur,
		Stitching,
		IntensityCorrection,
		Fusion,
		Export
	}

	private static final long serialVersionUID = 2619120742300093982L;

	private transient DataProvider dataProvider;
	private final DataAccessType dataAccessType;

	private final EnumSet< PipelineStep > pipeline;
	private StitchingArguments args;
	private SerializableStitchingParameters params;
	private final String baseFolder;

	private String saveFolder;
	private String datasetName;

	private transient List< TileInfo[] > tilesMultichannel;

	public StitchingJob( final StitchingArguments args )
	{
		this.args = args;
		ensureAbsolutePaths();

		dataProvider = DataProviderFactory.createByURI( URI.create( args.inputTileConfigurations().get( 0 ) ) );
		dataAccessType = dataProvider.getType();

		pipeline = setUpPipeline( args );

		final File inputFile = new File( args.inputTileConfigurations().get( 0 ) ).getAbsoluteFile();
		baseFolder = saveFolder = inputFile.getParent();
		datasetName = inputFile.getName();
		if ( datasetName.endsWith( ".json" ) )
			datasetName = datasetName.substring( 0, datasetName.lastIndexOf( ".json" ) );
	}

	private EnumSet< PipelineStep > setUpPipeline( final StitchingArguments args )
	{
		final List< PipelineStep > pipelineStepsList = new ArrayList<>();

		// mandatory step that validates tile configurations and tries to add some missing tiles, etc.
		pipelineStepsList.add( PipelineStep.Metadata );

		if ( !args.fuseOnly() )
			pipelineStepsList.add( PipelineStep.Stitching );

		if ( !args.stitchOnly() )
			pipelineStepsList.add( PipelineStep.Fusion );

		return EnumSet.copyOf( pipelineStepsList );
	}

	public EnumSet< PipelineStep > getPipeline() { return pipeline; }

	public synchronized DataProvider getDataProvider()
	{
		if ( dataProvider == null )
			dataProvider = DataProviderFactory.createByType( dataAccessType );
		return dataProvider;
	}

	public StitchingArguments getArgs() { return args; }

	public SerializableStitchingParameters getParams() { return params; }
	public void setParams( final SerializableStitchingParameters params ) { this.params = params; }

	public int getChannels() {
		return args.inputTileConfigurations().size();
	}

	public TileInfo[] getTiles( final int channel ) {
		return tilesMultichannel.get( channel );
	}

	public void setTiles( final TileInfo[] tiles, final int channel ) throws Exception {
		tilesMultichannel.set( channel, tiles );
		checkTilesConfiguration();
	}

	public void setTilesMultichannel( final List< TileInfo[] > tilesMultichannel ) throws Exception {
		this.tilesMultichannel = tilesMultichannel;
		checkTilesConfiguration();
	}

	public String getBaseFolder() { return baseFolder; }

	public String getSaveFolder() { return saveFolder; }
	public void setSaveFolder( final String saveFolder ) { this.saveFolder = saveFolder; }

	public String getDatasetName() { return datasetName; }

	public int getDimensionality() { return tilesMultichannel.get( 0 )[ 0 ].numDimensions(); }
	public double[] getPixelResolution() { return tilesMultichannel.get( 0 )[ 0 ].getPixelResolution(); }

	public void validateTiles() throws IllegalArgumentException
	{
		final int dimensionality = getDimensionality();

		double[] pixelResolution = getPixelResolution();
		if ( pixelResolution == null )
			pixelResolution = new double[] { 0.097, 0.097, 0.18 };

		for ( final TileInfo[] tiles : tilesMultichannel )
		{
			if ( tiles.length < 2 )
				throw new IllegalArgumentException( "There must be at least 2 tiles in the dataset" );

			for ( int i = 0; i < tiles.length; i++ )
				if ( tiles[ i ].getStagePosition().length != tiles[ i ].getSize().length )
					throw new IllegalArgumentException( "Incorrect dimensionality" );

			for ( int i = 1; i < tiles.length; i++ )
				if ( tiles[ i ].numDimensions() != tiles[ i - 1 ].numDimensions() )
					throw new IllegalArgumentException( "Incorrect dimensionality" );

			for ( final TileInfo tile : tiles )
				if ( tile.getPixelResolution() == null )
					tile.setPixelResolution( pixelResolution.clone() );

			if ( dimensionality != tiles[ 0 ].numDimensions() )
				throw new IllegalArgumentException( "Channels have different dimensionality" );
		}

		if ( params != null )
			params.dimensionality = dimensionality;
	}

	private void ensureAbsolutePaths()
	{
		// input configuration paths
		for ( int ch = 0; ch < args.inputTileConfigurations().size(); ++ch )
			if ( !Paths.get( args.inputTileConfigurations().get( ch ) ).isAbsolute() )
				args.inputTileConfigurations().set( ch, Paths.get( args.inputTileConfigurations().get( ch ) ).toAbsolutePath().toString() );

		// tile image paths
		if ( tilesMultichannel != null )
		{
			for ( int ch = 0; ch < tilesMultichannel.size(); ++ch )
			{
				for ( final TileInfo tile : tilesMultichannel.get( ch ) )
					if ( !Paths.get( tile.getFilePath() ).isAbsolute() )
						tile.setFilePath( Paths.get( args.inputTileConfigurations().get( ch ) ).getParent().resolve( tile.getFilePath() ).toString() );
			}
		}
	}

	private void checkTilesConfiguration() throws Exception
	{
		ensureAbsolutePaths();

		for ( final TileInfo[] tiles : tilesMultichannel )
		{
			boolean malformed = ( tiles == null );
			if ( !malformed )
				for ( final TileInfo tile : tiles )
					if ( tile == null )
						malformed = true;

			if ( malformed )
				throw new NullPointerException( "Malformed input" );

			for ( int i = 0; i < tiles.length; i++ ) {
				if ( tiles[ i ].getFilePath() == null || tiles[ i ].getStagePosition() == null )
					throw new NullPointerException( "Some of required parameters are missing (file or position)" );

				if ( tiles[ i ].getIndex() == null )
					tiles[ i ].setIndex( i );
			}
		}
	}
}
